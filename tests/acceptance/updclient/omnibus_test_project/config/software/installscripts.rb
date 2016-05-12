name "installscripts"
default_version "0.0.1"

build do
    installscripts_dir = "#{windows_safe_path(install_dir)}\\embedded\\installscripts"
    mkdir "#{installscripts_dir}"
    if ENV['USE_BROKEN'] == 'true'
        prefix = 'broken_'
    else
        prefix = ''
    end
    installscripts = "#{prefix}msi_install_actions.py"
    block do
        ["#{installscripts}", "windows_helpers.py" ].each do |name|
            contents = File.read("#{project.package_scripts_path}\\#{name}")
            out_file = File.new("#{installscripts_dir}\\#{name}", "w")
            out_file.puts(contents)
            out_file.close
        end
    end
end
