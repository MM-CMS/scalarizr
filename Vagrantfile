# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = {
  "ubuntu" => "ubuntu1204",
  "centos" => "centos63",
  "centos5" => "centos59",
  "amzn" => "amzn1303"
}

Vagrant.configure("2") do |config|
  boxes.each do |name, box|
    config.vm.define name do |machine|
      machine.vm.box = box
      machine.vm.provision :chef_client do |chef|
        chef.chef_server_url = "http://sl5.scalr.net:4000"
        chef.node_name = "#{ENV['USER']}.scalarizr-#{machine.vm.box}-vagrant"
        chef.validation_client_name = "chef-validator"
        chef.run_list = ["recipe[vagrant_boxes]"]
        chef.validation_key_path = "validation.pem"
      end

      if name == "amzn"
        machine.vm.provider :aws do |aws|
          aws.access_key_id = ENV['EC2_ACCESS_KEY']
          aws.secret_access_key = ENV['EC2_SECRET_KEY']
          aws.keypair_name = "vagrant"
          aws.ssh_private_key_path = ENV['EC2_VAGRANT_SSH_KEY']
          aws.ssh_username = "root"
          aws.ami = "ami-d884e1b1"
        end      
      end
    end
  end
end
