@haproxy
Feature: Parsing configuration haproxy
    In order to test the HaProxy configuration parser
    I use the lens haproxy.aug from /share/haproxy_lens
    Reference tree, copied from "augtool print" output without path and config name like: global/#comment = "comment"

    @top-level
    Scenario: The top-level options
        Given I have a sample haproxy configuration:
            """
            global
                # global section comment
            defaults
                # defaults section comment
            frontend http-in
                # frontend section comment
            backend servers
                # backend section comment
            listen http-in
                # listen section comment
            """
        When I parse it
        Then I get such tree:
            """
            global
            global/#comment = "global section comment"
            defaults
            defaults/#comment = "defaults section comment"
            frontend
            frontend/name = "http-in"
            frontend/#comment = "frontend section comment"
            backend
            backend/name = "servers"
            backend/#comment = "backend section comment"
            listen
            listen/name = "http-in"
            listen/#comment = "listen section comment"
            """
        When I change some options in sample config to:
            """
            frontend/name = "sample-http-in"
            backend/#comment = "backend comment"
            listen/name = "sample-http-in"
            """
        And I parse it
        Then I get such tree:
            """
            global
            global/#comment = "global section comment"
            defaults
            defaults/#comment = "defaults section comment"
            frontend
            frontend/name = "sample-http-in"
            frontend/#comment = "frontend section comment"
            backend
            backend/name = "servers"
            backend/#comment = "backend comment"
            listen
            listen/name = "sample-http-in"
            listen/#comment = "listen section comment"
            """

    @global
    Scenario: The options from global section
        Given I have a sample haproxy configuration:
            """
            global
                # Comment
                log 10.10.10.1   syslog warning
                log 10.10.10.2   daemon notice
                chroot /var/lib/haproxy
                pidfile /tmp/sample.pid
                spread-checks 5
                maxconn 4096
                user haproxy # Commet
                group haproxy
                daemon
                quiet
            """
        When I parse it
        Then I get such tree:
            """
            global
            global/#comment[1] = "Comment"
            global/log[1]
            global/log[1]/address = "10.10.10.1"
            global/log[1]/facility = "syslog"
            global/log[1]/level = "warning"
            global/log[2]
            global/log[2]/address = "10.10.10.2"
            global/log[2]/facility = "daemon"
            global/log[2]/level = "notice"
            global/chroot = "/var/lib/haproxy"
            global/pidfile = "/tmp/sample.pid"
            global/spread-checks = "5"
            global/maxconn = "4096"
            global/user = "haproxy"
            global/#comment[2] = "Commet"
            global/group = "haproxy"
            global/daemon
            global/quiet
            """
        When I change some options in sample config to:
            """
            global/log[1]/level = "info"
            global/chroot = "/tmp/haproxy"
            global/spread-checks = "500"
            global/log[2]/address = "10.10.10.10"
            """
        And I parse it
        Then I get such tree:
            """
            global
            global/#comment[1] = "Comment"
            global/log[1]
            global/log[1]/address = "10.10.10.1"
            global/log[1]/facility = "syslog"
            global/log[1]/level = "info"
            global/log[2]
            global/log[2]/address = "10.10.10.10"
            global/log[2]/facility = "daemon"
            global/log[2]/level = "notice"
            global/chroot = "/tmp/haproxy"
            global/pidfile = "/tmp/sample.pid"
            global/spread-checks = "500"
            global/maxconn = "4096"
            global/user = "haproxy"
            global/#comment[2] = "Commet"
            global/group = "haproxy"
            global/daemon
            global/quiet
            """

    @defaults
    Scenario: The options from defaults section
        Given I have a sample haproxy configuration:
            """
             defaults
                # Comment
                balance roundrobin
                errorfile 503 /etc/haproxy/errors/503.http
                log global
                maxconn 256000
                option http-server-close
                retries 3
                srvtimeout 50000
                stats auth username:password
                stats enable
                stats realm Haproxy\ Statistics
                stats uri /stats
                timeout server 30s
            userlist shareaholic_admins
                user myuser insecure-password mypass
            """
        When I parse it
        Then I get such tree:
            """
            defaults
            defaults/#comment = "Comment"
            defaults/balance
            defaults/balance/algorithm = "roundrobin"
            defaults/errorfile
            defaults/errorfile/code = "503"
            defaults/errorfile/file = "/etc/haproxy/errors/503.http"
            defaults/log = "global"
            defaults/maxconn = "256000"
            defaults/http-server-close = "true"
            defaults/retries = "3"
            defaults/srvtimeout = "50000"
            defaults/stats_auth
            defaults/stats_auth/user = "username"
            defaults/stats_auth/passwd = "password"
            defaults/stats_enable
            defaults/stats_realm = "Haproxy\\ Statistics"
            defaults/stats_uri = "/stats"
            defaults/timeout_server = "30s"
            userlist
            userlist/name = "shareaholic_admins"
            userlist/user
            userlist/user/name = "myuser"
            userlist/user/insecure-password = "mypass"
            """
        When I change some options in sample config to:
            """
            defaults/errorfile/code = "404"
            defaults/errorfile/file = "/etc/haproxy/errors/404.http"
            defaults/stats_uri = "/tmp/sample/"
            userlist/name = "sample_admin"
            userlist/user/insecure-password = "sample_pass"
            """
        And I parse it
        Then I get such tree:
            """
            defaults
            defaults/#comment = "Comment"
            defaults/balance
            defaults/balance/algorithm = "roundrobin"
            defaults/errorfile
            defaults/errorfile/code = "404"
            defaults/errorfile/file = "/etc/haproxy/errors/404.http"
            defaults/log = "global"
            defaults/maxconn = "256000"
            defaults/http-server-close = "true"
            defaults/retries = "3"
            defaults/srvtimeout = "50000"
            defaults/stats_auth
            defaults/stats_auth/user = "username"
            defaults/stats_auth/passwd = "password"
            defaults/stats_enable
            defaults/stats_realm = "Haproxy\\ Statistics"
            defaults/stats_uri = "/tmp/sample/"
            defaults/timeout_server = "30s"
            userlist
            userlist/name = "sample_admin"
            userlist/user
            userlist/user/name = "myuser"
            userlist/user/insecure-password = "sample_pass"
            """

    @frontend
    Scenario: The options from frontend section
        Given I have a sample haproxy configuration:
            """
            frontend ft-1-http-in
                bind *:80
                # Define hosts
                acl host_bacon hdr(host) -i ilovebacon.com
                acl host_milkshakes hdr(host) -i bobsmilkshakes.com
                reqadd X-Forwarded-Proto:\ http
                acl sharingthetech path_beg /tech
                acl tintoretto_app hdr_dom(host) -i tintoretto
                acl php_app path_beg /about /contact /help /media
                acl php_app path /api /api/ /tools/browser /tools/browser/
                acl php_app hdr_dom(host) -i blog.shareaholic.com

                ## figure out which one to use
                use_backend bacon_cluster if host_bacon
                use_backend milshake_cluster if host_milkshakes

            frontend ft-2-http-in
                bind 10.0.0.1:6379 name redis
                default_backend bk_redis#bind 10.0.24.100:8443 ssl crt /opt/local/haproxy/etc/data.pem
                mode http
                option httplog
                acl good_ips src -f /opt/local/haproxy/etc/gip.lst
                block if !good_ips
            """
        When I parse it
        Then I get such tree:
            """
            frontend[1]
            frontend[1]/name = "ft-1-http-in"
            frontend[1]/bind
            frontend[1]/bind/bind_addr
            frontend[1]/bind/bind_addr/address = "*"
            frontend[1]/bind/bind_addr/port = "80"
            frontend[1]/#comment[1] = "Define hosts"
            frontend[1]/acl[1]
            frontend[1]/acl[1]/name = "host_bacon"
            frontend[1]/acl[1]/value = "hdr(host) -i ilovebacon.com"
            frontend[1]/acl[2]
            frontend[1]/acl[2]/name = "host_milkshakes"
            frontend[1]/acl[2]/value = "hdr(host) -i bobsmilkshakes.com"
            frontend[1]/reqadd = "X-Forwarded-Proto:\\ http"
            frontend[1]/acl[3]
            frontend[1]/acl[3]/name = "sharingthetech"
            frontend[1]/acl[3]/value = "path_beg /tech"
            frontend[1]/acl[4]
            frontend[1]/acl[4]/name = "tintoretto_app"
            frontend[1]/acl[4]/value = "hdr_dom(host) -i tintoretto"
            frontend[1]/acl[5]
            frontend[1]/acl[5]/name = "php_app"
            frontend[1]/acl[5]/value = "path_beg /about /contact /help /media"
            frontend[1]/acl[6]
            frontend[1]/acl[6]/name = "php_app"
            frontend[1]/acl[6]/value = "path /api /api/ /tools/browser /tools/browser/"
            frontend[1]/acl[7]
            frontend[1]/acl[7]/name = "php_app"
            frontend[1]/acl[7]/value = "hdr_dom(host) -i blog.shareaholic.com"
            frontend[1]/#comment[2] = "# figure out which one to use"
            frontend[1]/use_backend[1] = "bacon_cluster"
            frontend[1]/use_backend[1]/if = "host_bacon"
            frontend[1]/use_backend[2] = "milshake_cluster"
            frontend[1]/use_backend[2]/if = "host_milkshakes"
            frontend[2]
            frontend[2]/name = "ft-2-http-in"
            frontend[2]/bind
            frontend[2]/bind/bind_addr
            frontend[2]/bind/bind_addr/address = "10.0.0.1"
            frontend[2]/bind/bind_addr/port = "6379"
            frontend[2]/bind/name = "redis"
            frontend[2]/default_backend = "bk_redis"
            frontend[2]/#comment = "bind 10.0.24.100:8443 ssl crt /opt/local/haproxy/etc/data.pem"
            frontend[2]/mode = "http"
            frontend[2]/httplog
            frontend[2]/acl
            frontend[2]/acl/name = "good_ips"
            frontend[2]/acl/value = "src -f /opt/local/haproxy/etc/gip.lst"
            frontend[2]/block
            frontend[2]/block/condition = "if !good_ips"
            """
        When I change some options in sample config to:
            """
            frontend[1]/bind/bind_addr/port = "8080"
            frontend[1]/reqadd = "X-Forwarded-Proto:\ https"
            frontend[2]/block/condition = "if !sample"
            frontend[2]/bind/bind_addr/address = "10.0.0.10"
            """
        And I parse it
        Then I get such tree:
            """
            frontend[1]
            frontend[1]/name = "ft-1-http-in"
            frontend[1]/bind
            frontend[1]/bind/bind_addr
            frontend[1]/bind/bind_addr/address = "*"
            frontend[1]/bind/bind_addr/port = "8080"
            frontend[1]/#comment[1] = "Define hosts"
            frontend[1]/acl[1]
            frontend[1]/acl[1]/name = "host_bacon"
            frontend[1]/acl[1]/value = "hdr(host) -i ilovebacon.com"
            frontend[1]/acl[2]
            frontend[1]/acl[2]/name = "host_milkshakes"
            frontend[1]/acl[2]/value = "hdr(host) -i bobsmilkshakes.com"
            frontend[1]/reqadd = "X-Forwarded-Proto:\\ https"
            frontend[1]/acl[3]
            frontend[1]/acl[3]/name = "sharingthetech"
            frontend[1]/acl[3]/value = "path_beg /tech"
            frontend[1]/acl[4]
            frontend[1]/acl[4]/name = "tintoretto_app"
            frontend[1]/acl[4]/value = "hdr_dom(host) -i tintoretto"
            frontend[1]/acl[5]
            frontend[1]/acl[5]/name = "php_app"
            frontend[1]/acl[5]/value = "path_beg /about /contact /help /media"
            frontend[1]/acl[6]
            frontend[1]/acl[6]/name = "php_app"
            frontend[1]/acl[6]/value = "path /api /api/ /tools/browser /tools/browser/"
            frontend[1]/acl[7]
            frontend[1]/acl[7]/name = "php_app"
            frontend[1]/acl[7]/value = "hdr_dom(host) -i blog.shareaholic.com"
            frontend[1]/#comment[2] = "# figure out which one to use"
            frontend[1]/use_backend[1] = "bacon_cluster"
            frontend[1]/use_backend[1]/if = "host_bacon"
            frontend[1]/use_backend[2] = "milshake_cluster"
            frontend[1]/use_backend[2]/if = "host_milkshakes"
            frontend[2]
            frontend[2]/name = "ft-2-http-in"
            frontend[2]/bind
            frontend[2]/bind/bind_addr
            frontend[2]/bind/bind_addr/address = "10.0.0.10"
            frontend[2]/bind/bind_addr/port = "6379"
            frontend[2]/bind/name = "redis"
            frontend[2]/default_backend = "bk_redis"
            frontend[2]/#comment = "bind 10.0.24.100:8443 ssl crt /opt/local/haproxy/etc/data.pem"
            frontend[2]/mode = "http"
            frontend[2]/httplog
            frontend[2]/acl
            frontend[2]/acl/name = "good_ips"
            frontend[2]/acl/value = "src -f /opt/local/haproxy/etc/gip.lst"
            frontend[2]/block
            frontend[2]/block/condition = "if !sample"
            """

    @backend
    Scenario: The options from backend section
        Given I have a sample haproxy configuration:
            """
            backend application-backend
                redirect scheme https if !{ ssl_fc }
                balance leastconn
                option httpclose
                option forwardfor
                cookie JSESSIONID prefix
                server node1 10.0.0.1:8080 cookie A check
                server node1 10.0.0.2:8080 cookie A check

            backend riak_rest_backend
              mode http
              balance roundrobin
              option httpchk GET /ping
              option httplog
              server riak1 riak1.<FQDN>:8098 weight 1 maxconn 1024 check
              server rails-secondary-a-1 10.1.1.5:8080 check inter 5000 fastinter 1000 fall 1 rise 1 weight 1 maxconn 100
            """
        When I parse it
        Then I get such tree:
            """
            backend[1]
            backend[1]/name = "application-backend"
            backend[1]/redirect
            backend[1]/redirect/scheme
            backend[1]/redirect/to = "https"
            backend[1]/redirect/if = "!{ ssl_fc }"
            backend[1]/balance
            backend[1]/balance/algorithm = "leastconn"
            backend[1]/httpclose = "true"
            backend[1]/forwardfor
            backend[1]/cookie
            backend[1]/cookie/name = "JSESSIONID"
            backend[1]/cookie/method = "prefix"
            backend[1]/server[1]
            backend[1]/server[1]/name = "node1"
            backend[1]/server[1]/address = "10.0.0.1"
            backend[1]/server[1]/port = "8080"
            backend[1]/server[1]/cookie = "A"
            backend[1]/server[1]/check
            backend[1]/server[2]
            backend[1]/server[2]/name = "node1"
            backend[1]/server[2]/address = "10.0.0.2"
            backend[1]/server[2]/port = "8080"
            backend[1]/server[2]/cookie = "A"
            backend[1]/server[2]/check
            backend[2]
            backend[2]/name = "riak_rest_backend"
            backend[2]/mode = "http"
            backend[2]/balance
            backend[2]/balance/algorithm = "roundrobin"
            backend[2]/httpchk
            backend[2]/httpchk/method = "GET"
            backend[2]/httpchk/uri = "/ping"
            backend[2]/httplog
            backend[2]/server[1]
            backend[2]/server[1]/name = "riak1"
            backend[2]/server[1]/address = "riak1.<FQDN>"
            backend[2]/server[1]/port = "8098"
            backend[2]/server[1]/weight = "1"
            backend[2]/server[1]/maxconn = "1024"
            backend[2]/server[1]/check
            backend[2]/server[2]
            backend[2]/server[2]/name = "rails-secondary-a-1"
            backend[2]/server[2]/address = "10.1.1.5"
            backend[2]/server[2]/port = "8080"
            backend[2]/server[2]/check
            backend[2]/server[2]/inter = "5000"
            backend[2]/server[2]/fastinter = "1000"
            backend[2]/server[2]/fall = "1"
            backend[2]/server[2]/rise = "1"
            backend[2]/server[2]/weight = "1"
            backend[2]/server[2]/maxconn = "100"
            """
        When I change some options in sample config to:
            """
            backend[1]/balance/algorithm = "static-rr"
            backend[2]/mode = "https"
            backend[1]/server[2]/cookie = "C"
            backend[2]/server[2]/maxconn = "1000"
            """
        And I parse it
        Then I get such tree:
            """
            backend[1]
            backend[1]/name = "application-backend"
            backend[1]/redirect
            backend[1]/redirect/scheme
            backend[1]/redirect/to = "https"
            backend[1]/redirect/if = "!{ ssl_fc }"
            backend[1]/balance
            backend[1]/balance/algorithm = "static-rr"
            backend[1]/httpclose = "true"
            backend[1]/forwardfor
            backend[1]/cookie
            backend[1]/cookie/name = "JSESSIONID"
            backend[1]/cookie/method = "prefix"
            backend[1]/server[1]
            backend[1]/server[1]/name = "node1"
            backend[1]/server[1]/address = "10.0.0.1"
            backend[1]/server[1]/port = "8080"
            backend[1]/server[1]/cookie = "A"
            backend[1]/server[1]/check
            backend[1]/server[2]
            backend[1]/server[2]/name = "node1"
            backend[1]/server[2]/address = "10.0.0.2"
            backend[1]/server[2]/port = "8080"
            backend[1]/server[2]/cookie = "C"
            backend[1]/server[2]/check
            backend[2]
            backend[2]/name = "riak_rest_backend"
            backend[2]/mode = "https"
            backend[2]/balance
            backend[2]/balance/algorithm = "roundrobin"
            backend[2]/httpchk
            backend[2]/httpchk/method = "GET"
            backend[2]/httpchk/uri = "/ping"
            backend[2]/httplog
            backend[2]/server[1]
            backend[2]/server[1]/name = "riak1"
            backend[2]/server[1]/address = "riak1.<FQDN>"
            backend[2]/server[1]/port = "8098"
            backend[2]/server[1]/weight = "1"
            backend[2]/server[1]/maxconn = "1024"
            backend[2]/server[1]/check
            backend[2]/server[2]
            backend[2]/server[2]/name = "rails-secondary-a-1"
            backend[2]/server[2]/address = "10.1.1.5"
            backend[2]/server[2]/port = "8080"
            backend[2]/server[2]/check
            backend[2]/server[2]/inter = "5000"
            backend[2]/server[2]/fastinter = "1000"
            backend[2]/server[2]/fall = "1"
            backend[2]/server[2]/rise = "1"
            backend[2]/server[2]/weight = "1"
            backend[2]/server[2]/maxconn = "1000"
            """

    @listen
    Scenario: The options from backend section
        Given I have a sample haproxy configuration:
            """
            listen mysql-cluster
                bind 0.0.0.0:3306
                mode tcp
                balance roundrobin
                option mysql-check user root
                server db01 10.4.29.100:3306 check
                server db02 10.4.29.99:3306 check
                server db03 10.4.29.98:3306 check
            """
        When I parse it
        Then I get such tree:
            """
            listen
            listen/name = "mysql-cluster"
            listen/bind
            listen/bind/bind_addr
            listen/bind/bind_addr/address = "0.0.0.0"
            listen/bind/bind_addr/port = "3306"
            listen/mode = "tcp"
            listen/balance
            listen/balance/algorithm = "roundrobin"
            listen/mysql_check
            listen/mysql_check/user = "root"
            listen/server[1]
            listen/server[1]/name = "db01"
            listen/server[1]/address = "10.4.29.100"
            listen/server[1]/port = "3306"
            listen/server[1]/check
            listen/server[2]
            listen/server[2]/name = "db02"
            listen/server[2]/address = "10.4.29.99"
            listen/server[2]/port = "3306"
            listen/server[2]/check
            listen/server[3]
            listen/server[3]/name = "db03"
            listen/server[3]/address = "10.4.29.98"
            listen/server[3]/port = "3306"
            listen/server[3]/check
            """
        When I change some options in sample config to:
            """
            listen/mysql_check/user = "test"
            listen/server[3]/address = "10.4.29.99"
            listen/bind/bind_addr/address = "127.0.0.1"
            """
        And I parse it
        Then I get such tree:
            """
            listen
            listen/name = "mysql-cluster"
            listen/bind
            listen/bind/bind_addr
            listen/bind/bind_addr/address = "127.0.0.1"
            listen/bind/bind_addr/port = "3306"
            listen/mode = "tcp"
            listen/balance
            listen/balance/algorithm = "roundrobin"
            listen/mysql_check
            listen/mysql_check/user = "test"
            listen/server[1]
            listen/server[1]/name = "db01"
            listen/server[1]/address = "10.4.29.100"
            listen/server[1]/port = "3306"
            listen/server[1]/check
            listen/server[2]
            listen/server[2]/name = "db02"
            listen/server[2]/address = "10.4.29.99"
            listen/server[2]/port = "3306"
            listen/server[2]/check
            listen/server[3]
            listen/server[3]/name = "db03"
            listen/server[3]/address = "10.4.29.99"
            listen/server[3]/port = "3306"
            listen/server[3]/check
            """