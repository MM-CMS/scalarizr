name "python"
default_version "2.7.8"

if windows?
  source :url => "http://www.python.org/ftp/python/#{version}/python-#{version}.amd64.msi",
         :md5 => '38cadfcac6dd56ecf772f2f3f14ee846'
  relative_path "Python-#{version}"

  build do
    pythondir = "#{install_dir}/embedded"
    mkdir "#{pythondir}"
    command "msiexec /a python-#{version}.amd64.msi /qb TARGETDIR=#{windows_safe_path(pythondir)} ADDLOCAL=DefaultFeature"
  end

else
  dependency "gdbm"
  dependency "ncurses"
  dependency "zlib"
  dependency "openssl"
  dependency "bzip2"
  dependency "sqlite3"

  source url: "http://python.org/ftp/python/#{version}/Python-#{version}.tgz",
         md5: 'd4bca0159acb0b44a781292b5231936f'

  relative_path "Python-#{version}"

  build do
    env = {
      "CFLAGS" => "-I#{install_dir}/embedded/include -O3 -g -pipe",
      "LDFLAGS" => "-Wl,-rpath,#{install_dir}/embedded/lib -L#{install_dir}/embedded/lib"
    }

    command "./configure" \
            " --prefix=#{install_dir}/embedded" \
            " --enable-shared" \
            " --with-dbmliborder=gdbm", env: env

    make env: env
    make "install", env: env

    # There exists no configure flag to tell Python to not compile readline
    delete "#{install_dir}/embedded/lib/python2.7/lib-dynload/readline.*"

    # Remove unused extension which is known to make healthchecks fail on CentOS 6
    delete "#{install_dir}/embedded/lib/python2.7/lib-dynload/_bsddb.*"
    delete "#{install_dir}/embedded/lib/python2.7/bsddb"

    # Remove python own testsuite (~28M)
    delete "#{install_dir}/embedded/lib/python2.7/test"
  end
end
