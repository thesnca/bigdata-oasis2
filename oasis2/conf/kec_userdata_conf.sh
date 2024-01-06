#!/bin/bash

set -x
set -eu
set -o pipefail


# ssh 1505
sshd_conf_file="/etc/ssh/sshd_config"
sshd_conf_str="\n# KMR sshd_conf\nPort 22\nPort 1505\nPubkeyAuthentication yes\nPasswordAuthentication no\n"
if ! grep -q "# KMR sshd_conf" $sshd_conf_file;then
    echo -e $sshd_conf_str >> $sshd_conf_file
    systemctl restart sshd.service
fi

# ip_local_reserved_ports
sysctl_conf_file="/etc/sysctl.conf"
sysctl_conf_str="\n# KMR jmx_exporter\nnet.ipv4.ip_local_reserved_ports = 1320,8633,9000-9500\n"
if ! grep -q "# KMR jmx_exporter" $sysctl_conf_file;then
    echo -e $sysctl_conf_str >> $sysctl_conf_file
    sysctl -p
fi

# ssh key
public_key="%(public_key)s"
public_key_file="/root/.ssh/authorized_keys"

if [ -n "$public_key" ];then
    mkdir -p "/root/.ssh/"
    touch $public_key_file
    echo -e $public_key >> $public_key_file
    chmod 600 $public_key_file
fi

# mtod
motd_file="/etc/profile.d/motd.sh"
if [ ! -e $motd_file ]
  then touch $motd_file
fi

# # supervisor(supervisor-kes.sh会去下载和安装supervisor)
# supervisor_conf_file="/etc/supervisord.conf"
# if [ ! -e $supervisor_conf_file ];then
#     yum install -y supervisor
#     echo_supervisord_conf > $supervisor_conf_file
#     supervisord -c $supervisor_conf_file
#     systemctl enable supervisord
# fi

# # nginx(改成脚本安装，并且在安装agent之前)
# nginx_conf_file="/etc/nginx/nginx.conf"
# if [ ! -e $nginx_conf_file ];then
#     yum install -y nginx dnsmasq
#     systemctl enable nginx
#     systemctl start nginx
#     systemctl enable dnsmasq
#     systemctl start dnsmasq
# fi


cat > $motd_file <<-"EOF"
#!/bin/bash

echo "                                                                    ";
echo -e "\033[34mKKKKKKKKK    KKKKKKEEEEEEEEEEEEEEEEEEEEEE  SSSSSSSSSSSSSSS \033[0m";
echo -e "\033[34mK:::::::K    K:::::E::::::::::::::::::::ESS:::::::::::::::S  \033[0m";
echo -e "\033[34mK:::::::K    K:::::E::::::::::::::::::::S:::::SSSSSS::::::S\033[0m";
echo -e "\033[34mK:::::::K   K::::::EE::::::EEEEEEEEE::::S:::::S     SSSSSSS\033[0m";
echo -e "\033[34mKK::::::K  K:::::KKK E:::::E       EEEEES:::::S            \033[0m";
echo -e "\033[34m  K:::::K K:::::K    E:::::E            S:::::S            \033[0m";
echo -e "\033[34m  K::::::K:::::K     E::::::EEEEEEEEEE   S::::SSSS         \033[0m";
echo -e "\033[34m  K:::::::::::K      E:::::::::::::::E    SS::::::SSSSS    \033[0m";
echo -e "\033[34m  K:::::::::::K      E:::::::::::::::E      SSS::::::::SS  \033[0m";
echo -e "\033[34m  K::::::K:::::K     E::::::EEEEEEEEEE         SSSSSS::::S \033[0m";
echo -e "\033[34m  K:::::K K:::::K    E:::::E                        S:::::S\033[0m";
echo -e "\033[34mKK::::::K  K:::::KKK E:::::E       EEEEEE           S:::::S\033[0m";
echo -e "\033[34mK:::::::K   K::::::EE::::::EEEEEEEE:::::SSSSSSS     S:::::S\033[0m";
echo -e "\033[34mK:::::::K    K:::::E::::::::::::::::::::S::::::SSSSSS:::::S\033[0m";
echo -e "\033[34mK:::::::K    K:::::E::::::::::::::::::::S:::::::::::::::SS \033[0m";
echo -e "\033[34mKKKKKKKKK    KKKKKKEEEEEEEEEEEEEEEEEEEEEESSSSSSSSSSSSSSS   \033[0m";
echo "                                                                    ";
echo -e "You have logged in as \033[31m$(whoami)\033[0m. "
if [ $(whoami) = "root" ]; then
    echo -e "To manage elasticsearch services and jobs, please switch to service accounts (elasticsearch) using following command:"
    echo -e "\033[33m    # su - SERVICE_ACCOUNT \033[0m"
fi
EOF
