name "test"
default_version "0.0.1"

dependency "python"
dependency "installscripts"

if windows?
    build do
        command "echo #{project.resources_path}"
        block do
            out_file = File.new("#{install_dir}/file.txt", "w")
            out_file.puts("This is a test file. Version #{project.build_version}")
            out_file.close
        end
    end
end
