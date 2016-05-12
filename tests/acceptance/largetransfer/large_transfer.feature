Feature: Large transfer

Scenario: Upload one stream with gzipping
    Given I have 1 stream(s)
    When I upload stream(s) to Storage with gzipping True
    Then I expect there is no error
    Then I expect manifest as a result
    And All data are uploaded
    And Temporary files are deleted

Scenario: Upload one stream without gzipping
    Given I have 1 stream(s)
    When I upload stream(s) to Storage with gzipping False
    Then I expect there is no error
    Then I expect manifest as a result
    And All data are uploaded
    And Temporary files are deleted

Scenario: Upload many streams with gzipping
    Given I have 5 stream(s)
    When I upload stream(s) to Storage with gzipping True
    Then I expect there is no error
    Then I expect manifest as a result
    And All data are uploaded
    And Temporary files are deleted

Scenario: Upload many streams without gzipping
    Given I have 5 stream(s)
    When I upload stream(s) to Storage with gzipping False
    Then I expect there is no error
    Then I expect manifest as a result
    And All data are uploaded
    And Temporary files are deleted

Scenario: Download stream
    Given I have uploaded stream with gzipping True
    When I download with the manifest
    Then I expect there is no error
    Then I expect original items are downloaded
    And Temporary files are deleted

Scenario: Simple upload one file
    Given I have 1 file(s)
    When I upload file(s) to Storage with simple True
    Then I expect there is no error
    Then All data are uploaded
    And Temporary files are deleted

Scenario: Simple upload many files
    Given I have 10 file(s)
    When I upload file(s) to Storage with simple True
    Then I expect there is no error
    Then All data are uploaded
    And Temporary files are deleted

Scenario: Upload one file
    Given I have 1 file(s)
    When I upload file(s) to Storage with simple False
    Then I expect there is no error
    Then All data are uploaded
    And Temporary files are deleted

Scenario: Upload many files
    Given I have 10 file(s)
    When I upload file(s) to Storage with simple False
    Then I expect there is no error
    Then All data are uploaded
    And Temporary files are deleted

Scenario: Download file
    Given I have uploaded file with simple True
    When I download file
    Then I expect there is no error
    Then I expect original items are downloaded
   
Scenario: Compatibility with the old manifest
    Given I have uploaded stream with old manifest
    When I download with the manifest
    Then I expect there is no error
    Then I expect original items are downloaded
    And Temporary files are deleted

Scenario: Simple upload one file with driver error
    Given I have 1 file(s)
    Given I have error ValueError in driver
    When I upload file(s) to Storage with simple True
    Then I get TransferError ValueError
    And Temporary files are deleted

Scenario: Download then one ore more chunks are missing
    Given I have uploaded stream with gzipping True
    When I remove one chunk
    When I download with the manifest
    Then I get TransferError DriverError
    And Temporary files are deleted

Scenario: Upload with terminate
    Given I have 100MB file
    When I start upload file(s) to Storage with simple True
    Then I wait while upload starts
    Then I wait 2 second(s)
    Then I stop upload
    Then I expect uploading is stopped
    And Temporary files are deleted

Scenario: md5sum check
    Given I have uploaded stream with gzipping True
    When I replace one chunk
    When I download with the manifest
    Then I get TransferError MD5SumError
    And Temporary files are deleted

Scenario: Compatibility with old LargeTransfer
    Given I have uploaded stream with gzipping True
    When I download with old LargeTransfer
    Then I expect original items are downloaded
