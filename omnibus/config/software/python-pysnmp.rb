name "python-pysnmp"
pypi_name = "pysnmp"
default_version "4.2.4"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
