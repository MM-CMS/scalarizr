
name            "mock-scalarizr"
maintainer    "test Inc"
homepage    "http://test.com"


build_version   ENV['OMNIBUS_BUILD_VERSION']
install_dir      File.absolute_path("/opt/mock-scalarizr/#{build_version}")
build_iteration 1

if windows?
  package :msi do
    upgrade_code '2CD7259C-776D-4DDB-A4C8-6E544E580AA1'
    wix_light_extension "WixUtilExtension"
  end
end

dependency "preparation"
dependency "test"
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
