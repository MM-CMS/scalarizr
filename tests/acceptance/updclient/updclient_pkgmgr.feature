@pkgmgr
Feature: UpdateClient: Lightweight package manager
    In order to get package info i use status
    In order to download package i use fetch
    In order to install package i use install
    In order to uninstall package i use uninstall or purge

    @pkgmgr
    Scenario Outline: Using custom package-manager utility for <repotype>
        Given I have two available packages from <repo>
        When I get first package
        Then I install first package
        And I check first package installation sequence
        Then I upgrade first package to the second
        And I check second package installation sequence
        Then I downgrade from the second package to the first
        Then I remove the first package

        Examples: Repo list
        | repotype | repo                                                                                   |
        | RedHat   | http://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/rpm/               |
        | Debian   | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/deb test main     |
        | Windows  | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/win               |

    @pkgmgr @windows
    Scenario Outline: Upgrade after rollback on Windows
        Given I have a scalarizr repository "<repo>"
        And I have installed base version of scalarizr
        When it tries to update base with the broken version
        Then it rolls back to the previous vesion
        When it tries to update base with the good version
        Then it installs new version of scalarizr

        Examples: Repo list
        | repo                                                                      |
        | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo4/win  |
