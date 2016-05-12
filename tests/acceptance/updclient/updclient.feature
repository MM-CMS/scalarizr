@updclient
Feature: Update Scalarizr package
    As I start updclient for the first time
    It should update the scalarizr package to the latest version available

    Scenario Outline: Bootstraping updclient on <repotype>
        Given I have a scalarizr repository "<repo>" with the latest version "<version>"
        When I bootstrap updclient
        Then it installs the latest version of scalarizr from the given repository

        Examples: Repo list
        | repotype      | version    | repo                                                                                   |
        | RedHat        | 102.0      | http://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/rpm/               |
        | Debian        | 102.0      | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/deb test main     |
        | Windows       | 10.0.0.2   | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo1/win/              |

    Scenario Outline: Updating package on <repotype>
        Given I have a scalarizr repository "<repo>" with the latest version "<version>"
        When I call update
        Then it installs the latest version of scalarizr from the given repository

        Examples: Repo list
        | repotype      | version    | repo                                                                                   |
        | RedHat        | 200.0      | http://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo2/rpm/               |
        | Debian        | 200.0      | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo2/deb test main     |
        | Windows       | 20.0.0.0   | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo2/win/              |

    Scenario Outline: Rallback on <repotype>
        Given I have a scalarizr repository "<repo>" with a broken version of scalarizr
        When I call update
        Then it tries to install the broken version and rollbacks to the previous vesion "<version>"

        Examples: Repo list
        | repotype      | version    | repo                                                                                   |
        | RedHat        | 200.0      | http://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo2/rpm/               |
        | Debian        | 200.0      | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo2/deb test main     |
        | Windows       | 20.0.0.0   | https://s3.amazonaws.com/scalr-labs/fixtures/scalarizer/pkgmgr/repo3/win/              |
