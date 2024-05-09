Feature: Create Bucket

  Scenario Outline: User role tries to create a bucket
    Given an authenticated user with "<Role>" and <Permissions>
    When the user sends a request to create a bucket with the name "newbucket"
    Then the response should have a status code of <Status>
    And the response should <Outcome>

    Examples:
      | Role          | Permissions   | Status | Outcome                                |
      | service_key   |               | 200    | confirm that the bucket "newbucket" was created |
      | authenticated | create_bucket | 200    | confirm that the bucket "newbucket" was created |
      | anon          |               | 403    | indicate "Permission Denied"          |

  Scenario: User tries to create a bucket without authentication header
    When the user sends a request to create a bucket with the name "newbucket1" without an authentication header
    Then the response should have a status code of 400

  Scenario: User tries to create a bucket with a duplicate name
    Given the user is authenticated
    And the bucket "bucket2" already exists
    When the user sends a request to create a bucket with the name "bucket2"
    Then the response should have a status code of 400