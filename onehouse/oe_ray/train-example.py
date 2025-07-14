from typing import Tuple

import pandas as pd
import xgboost

import ray
from ray import serve
from ray.data import Dataset
from ray.data import Dataset, Preprocessor
from ray.data.preprocessors import StandardScaler
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.xgboost import XGBoostTrainer

import os

def prepare_data() -> Tuple[Dataset, Dataset, Dataset]:
    """Load and split the dataset into train, validation, and test sets."""
    dataset = ray.data.read_hudi(
        table_uri="s3a://onehouse-customer-bucket-7a00bf9c/datalake/cancer_prediction/breast_cancer_data/v1",
        storage_options={"aws_region": "us-west-2"}
    )

    # Drop hoodie metadata cols
    dataset = dataset.drop_columns([
        "_hoodie_file_name", "_hoodie_record_key",
        "_hoodie_commit_seqno", "_hoodie_commit_time",
        "_hoodie_partition_path"
    ])
    
    # Cast cols to numeric
    feature_cols = [col for col in dataset.schema().names if col != "target"]
    def cast_to_numeric(batch: dict) -> dict:
        new_batch = {}
        for col, values in batch.items():
            if col in feature_cols:
                new_batch[col] = pd.to_numeric(values, errors='coerce')
            elif col == "target":
                new_batch[col] = pd.to_numeric(values, errors='coerce').astype(int)
            else:
                new_batch[col] = values
        return new_batch

    dataset = dataset.map_batches(cast_to_numeric)

    print('---------------SAMPLE-DATA----------------')
    print(dataset.take_batch(2))
    print('---------------DATA-SCHEMA----------------')
    print(dataset.schema())
    print('------------------------------------------')

    train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)
    test_dataset = valid_dataset.drop_columns(["target"])

    return train_dataset, valid_dataset, test_dataset

# Initialize Ray
ray.init()

# Load and split the dataset
train_dataset, valid_dataset, test_dataset = prepare_data()

# Pick some dataset columns to scale
columns_to_scale = ["mean_radius", "mean_texture"]

# Initialize and train the preprocessor
preprocessor = StandardScaler(columns=columns_to_scale)
preprocessor.fit(train_dataset)
train_dataset = preprocessor.transform(train_dataset)
valid_dataset = preprocessor.transform(valid_dataset)

# Configure checkpointing
run_config = RunConfig(
    storage_path="s3://onehouse-customer-bucket-7a00bf9c/models/breast_cancer_prediction/",
    checkpoint_config=CheckpointConfig(
        checkpoint_frequency=10,
        num_to_keep=1,
    )
)

# Set up the XGBoost trainer
trainer = XGBoostTrainer(
    scaling_config=ScalingConfig(
        num_workers=2,
        use_gpu=False,
    ),
    label_column="target",
    num_boost_round=20,
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset, "valid": valid_dataset},
    metadata={"preprocessor_pkl": preprocessor.serialize()},
    run_config=run_config,
)

# Train the model
result = trainer.fit()
print(result.metrics)

# -------------------------------
# Run prediction with the model
# -------------------------------

class Predict:
    def __init__(self, checkpoint: Checkpoint):
        self.model = XGBoostTrainer.get_model(checkpoint)
        # extract the preprocessor from the checkpoint metadata
        self.preprocessor = Preprocessor.deserialize(
            checkpoint.get_metadata()["preprocessor_pkl"]
        )

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        preprocessed_batch = self.preprocessor.transform_batch(batch)
        dmatrix = xgboost.DMatrix(preprocessed_batch)
        return pd.DataFrame({"predictions": self.model.predict(dmatrix)})

scores = test_dataset.map_batches(
    Predict,
    fn_constructor_args=[result.checkpoint],
    concurrency=1,
    batch_format="pandas",
)

predicted_labels = scores.map_batches(
    lambda df: pd.DataFrame({"predicted_label": (df["predictions"] > 0.5).astype(int)}),
    batch_format="pandas"
)
print("PREDICTED LABELS")
predicted_labels.show()


# -------------------------------
# Serve the trained model
# -------------------------------

# # Start Ray Serve (detached so it survives across sessions if needed)
# serve.start(detached=True)

# # Define a Serve deployment
# class XGBoostModelDeployment:
#     def __init__(self, checkpoint: Checkpoint):
#         self.model = self._load_model(checkpoint)
#         self.preprocessor = StandardScaler.deserialize(
#             checkpoint.get_metadata()["preprocessor_pkl"]
#         )

#     def _load_model(self, checkpoint: Checkpoint) -> xgb.Booster:
#         model_path = checkpoint.get_path()
#         booster = xgb.Booster()
#         booster.load_model(os.path.join(model_path, "model.xgb"))
#         return booster

#     async def __call__(self, request):
#         data = await request.json()
#         df = pd.DataFrame([data])
#         df = self.preprocessor.transform_pandas(df)
#         dmatrix = xgb.DMatrix(df)
#         pred = self.model.predict(dmatrix)
#         return {"prediction": float(pred[0])}

# # Start serving if not already started
# serve.start(detached=True)

# # Deployment definition
# deployment = serve.deployment(
#     XGBoostModelDeployment,
#     ray_actor_options={"num_cpus": 1}
# )

# # Bind the checkpoint and run the deployment
# bound_deployment = deployment.bind(result.checkpoint)
# serve.run(bound_deployment, name="xgb_model")

# print("Model is now served at http://127.0.0.1:8000/xgb_model")