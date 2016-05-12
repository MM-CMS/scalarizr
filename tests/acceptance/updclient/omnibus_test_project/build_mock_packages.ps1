#!powershell
$mock_repo_dir = "C:\Temp\omnibus_test_project"
$broken_version = '30.0.0.0'
$versions = '10.0.0.0', '10.0.0.1', '10.0.0.2', '20.0.0.0', $broken_version
$project_name = 'mock-scalarizr'

$env:INSTALL_DIR = "/opt/scalarizr"

cd $mock_repo_dir
git reset --hard
git pull
bundle install --binstubs
$env:USE_BROKEN = 'false'
ruby .\bin\omnibus clean $project_name --log-level=debug --purge
foreach ($versn in $versions) {
    $env:OMNIBUS_BUILD_VERSION=$versn
    $env:MSI_VERSION=$versn
    $env:VERSION=$versn
    if ($versn -eq $broken_version) {
        $env:USE_BROKEN = 'true'
    }
    ruby .\bin\omnibus clean $project_name --log-level=debug
    ruby .\bin\omnibus build $project_name --log-level=debug

}
