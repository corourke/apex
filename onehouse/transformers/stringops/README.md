# StringOps transformer

### Install

`$ mvn package clean`
Upload the `stringops-0.1.jar` file to Onehouse, Settings -> Integrations -> Manage JARs.

### Usage

Below are the parameters needed to configure the transformer:

#### Common Configuration Keys

- **`stringop.operation`**  
  _Description:_ The string operation to perform.  
  _Valid Values:_

  - `trim` (removes both leading and trailing whitespace)
  - `ltrim` (removes leading whitespace)
  - `rtrim` (removes trailing whitespace)
  - `left` (extracts the leftmost _n_ characters)
  - `right` (extracts the rightmost _n_ characters)
  - `substring` (extracts a substring given a start index and length)
  - `toUpperCase` (converts the string to uppercase)
  - `toLowerCase` (converts the string to lowercase)
  - `replace` (replaces all occurrences of a target substring with a replacement)
  - `regexReplace` (replaces substrings matching a regular expression)
  - `padLeft` (pads the string on the left to a specified total length)
  - `padRight` (pads the string on the right to a specified total length)  
    _Example:_

  ```properties
  stringop.operation=left
  ```

- **`stringop.source.column`**  
  _Description:_ The name of the source column to operate on.  
  _Example:_

  ```properties
  stringop.source.column=full_name
  ```

- **`stringop.dest.column`**  
  _Description:_ The name of the destination column where the result will be stored.  
  _Example:_
  ```properties
  stringop.dest.column=first_name
  ```

---

### Operation-Specific Parameters

Depending on the chosen operation, additional parameters may be required. Use keys prefixed with `stringop.param.` to supply these values.

#### For **`left`** and **`right`**:

- **`stringop.param.length`**  
  _Description:_ Number of characters to extract from the left or right.  
  _Example:_
  ```properties
  stringop.param.length=5
  ```

#### For **`substring`**:

- **`stringop.param.start`**  
  _Description:_ Starting index (0-based) for extraction.  
  _Example:_
  ```properties
  stringop.param.start=2
  ```
- **`stringop.param.length`**  
  _Description:_ Number of characters to extract starting from the `start` index.  
  _Example:_
  ```properties
  stringop.param.length=4
  ```

#### For **`replace`**:

- **`stringop.param.target`**  
  _Description:_ The substring to be replaced.  
  _Example:_
  ```properties
  stringop.param.target=old
  ```
- **`stringop.param.replacement`**  
  _Description:_ The replacement string.  
  _Example:_
  ```properties
  stringop.param.replacement=new
  ```

#### For **`regexReplace`**:

- **`stringop.param.regex`**  
  _Description:_ The regular expression pattern to match.  
  _Example:_
  ```properties
  stringop.param.regex=\d+
  ```
- **`stringop.param.replacement`**  
  _Description:_ The replacement string for matches.  
  _Example:_
  ```properties
  stringop.param.replacement=#
  ```

#### For **`padLeft`** and **`padRight`**:

- **`stringop.param.totalLength`**  
  _Description:_ The desired total length of the string after padding.  
  _Example:_
  ```properties
  stringop.param.totalLength=10
  ```
- **`stringop.param.padChar`**  
  _Description:_ The character to use for padding.  
  _Example:_
  ```properties
  stringop.param.padChar=0
  ```

---

### Example Configuration

For instance, to extract the leftmost 5 characters from a column named `full_name` and store the result in `first_name`, you would configure:

```properties
stringop.operation=left
stringop.source.column=full_name
stringop.dest.column=first_name
stringop.param.length=5
```
