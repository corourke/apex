package ai.onehouse.transformers;

import java.util.regex.Pattern;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class StringOpTransformer implements Transformer {
  private static final Logger log = LogManager.getLogger(StringOpTransformer.class);

  @Override
  public Dataset<Row> apply(JavaSparkContext javaSparkContext, SparkSession sparkSession,
      Dataset<Row> dataset, TypedProperties typedProperties) {
    // Read the required configuration properties.
    String operation = typedProperties.getString("stringop.operation", "");
    String sourceColumn = typedProperties.getString("stringop.source.column", "");
    String destColumn = typedProperties.getString("stringop.dest.column", "");

    if (operation.isEmpty() || sourceColumn.isEmpty() || destColumn.isEmpty()) {
      log.error("Missing required configuration for StringOpTransformer. " +
          "Ensure 'stringop.operation', 'stringop.source.column', and 'stringop.dest.column' are provided.");
      return dataset;
    }

    log.info("StringOpTransformer: operation=" + operation +
        ", sourceColumn=" + sourceColumn + ", destColumn=" + destColumn);

    Column srcCol = dataset.col(sourceColumn);
    Column result = null;
    String op = operation.toLowerCase();

    try {
      if (op.equals("trim")) {
        result = functions.trim(srcCol);
      } else if (op.equals("ltrim")) {
        result = functions.ltrim(srcCol);
      } else if (op.equals("rtrim")) {
        result = functions.rtrim(srcCol);
      } else if (op.equals("left")) {
        // Requires stringop.param.length
        String lenStr = typedProperties.getString("stringop.param.length", "0");
        int len = Integer.parseInt(lenStr);
        result = functions.substring(srcCol, 1, len); // Spark substring is 1-indexed.
      } else if (op.equals("right")) {
        // Requires stringop.param.length.
        String lenStr = typedProperties.getString("stringop.param.length", "0");
        int len = Integer.parseInt(lenStr);
        // Use an SQL expression to compute the rightmost substring.
        result = functions.expr(
            "substring(" + sourceColumn + ", greatest(length(" + sourceColumn + ")-" + len + "+1, 1), " + len + ")");
      } else if (op.equals("substring")) {
        // Requires stringop.param.start (0-based) and stringop.param.length.
        String startStr = typedProperties.getString("stringop.param.start", "0");
        String lenStr = typedProperties.getString("stringop.param.length", "0");
        int start = Integer.parseInt(startStr);
        int subLen = Integer.parseInt(lenStr);
        // Adjust for Spark's 1-indexed substring.
        result = functions.substring(srcCol, start + 1, subLen);
      } else if (op.equals("touppercase") || op.equals("toupper") || op.equals("upper")) {
        result = functions.upper(srcCol);
      } else if (op.equals("tolowercase") || op.equals("tolower") || op.equals("lower")) {
        result = functions.lower(srcCol);
      } else if (op.equals("replace")) {
        // Requires stringop.param.target and stringop.param.replacement.
        String target = typedProperties.getString("stringop.param.target", "");
        String replacement = typedProperties.getString("stringop.param.replacement", "");
        // Use Pattern.quote to escape any regex meta-characters for literal
        // replacement.
        result = functions.regexp_replace(srcCol, Pattern.quote(target), replacement);
      } else if (op.equals("regexreplace")) {
        // Requires stringop.param.regex and stringop.param.replacement.
        String regex = typedProperties.getString("stringop.param.regex", "");
        String replacement = typedProperties.getString("stringop.param.replacement", "");
        result = functions.regexp_replace(srcCol, regex, replacement);
      } else if (op.equals("padleft")) {
        // Requires stringop.param.totalLength and stringop.param.padChar.
        String totalLengthStr = typedProperties.getString("stringop.param.totalLength", "0");
        int totalLength = Integer.parseInt(totalLengthStr);
        String padChar = typedProperties.getString("stringop.param.padChar", " ");
        result = functions.lpad(srcCol, totalLength, padChar);
      } else if (op.equals("padright")) {
        // Requires stringop.param.totalLength and stringop.param.padChar.
        String totalLengthStr = typedProperties.getString("stringop.param.totalLength", "0");
        int totalLength = Integer.parseInt(totalLengthStr);
        String padChar = typedProperties.getString("stringop.param.padChar", " ");
        result = functions.rpad(srcCol, totalLength, padChar);
      } else {
        log.error("Unknown string operation: " + operation);
        return dataset;
      }
    } catch (Exception e) {
      log.error("Error applying string operation: " + e.getMessage(), e);
      return dataset;
    }

    // Append the new column to the dataset.
    return dataset.withColumn(destColumn, result);
  }
}
