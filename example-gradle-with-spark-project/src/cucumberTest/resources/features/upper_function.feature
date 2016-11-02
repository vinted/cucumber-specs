Feature: upper() function testing example
  Background:
    Given Hive table "testing.inputs"
      | String | String                              |
      | input  | comment                             |
      | hello  |                                     |
      | world  |                                     |
      | null   | Regression for NullPointerException |
      |        | Regression for empty string         |

  Scenario: Testing various string inputs
    Then SQL query "select input, upper(input) output, comment from testing.inputs" includes the following subset:
      | String | String | String                              |
      | input  | output | comment                             |
      | hello  | HELLO  |                                     |
      | world  | WORLD  |                                     |
      | null   | null   | Regression for NullPointerException |
      |        |        | Regression for empty string         |
