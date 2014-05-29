# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2" # Vagrantfile API/syntax version. Don't touch unless you know what you're doing!

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  # Settings for all vms
  config.berkshelf.enabled = true

  # Handle local proxy settings
  if Vagrant.has_plugin?("vagrant-proxyconf")
    if ENV["http_proxy"]
      config.proxy.http = ENV["http_proxy"]
    end
    if ENV["https_proxy"]
      config.proxy.https = ENV["https_proxy"]
    end
    if ENV["no_proxy"]
      config.proxy.no_proxy = ENV["no_proxy"]
    end
  end

  config.vm.box = "precise64"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"
  config.vm.synced_folder "~/", "/vagrant_home"
  config.vm.provider "virtualbox" do |vb|
    vb.customize ["modifyvm", :id, "--memory", "2048"]
  end

  config.vm.define "vertica1" do |vertica|
    vertica.vm.hostname = 'vertica1.backuptest.com'
    vertica.vm.network :private_network, ip: "192.168.10.2"
    vertica.vm.provision :chef_solo do |chef|
      chef.roles_path = "roles"
      chef.data_bags_path = "data_bags"
      chef.add_role "Vertica"
    end
  end

  config.vm.define "vertica2" do |vertica|
    vertica.vm.hostname = 'vertica2.backuptest.com'
    vertica.vm.network :private_network, ip: "192.168.10.3"
    vertica.vm.provision :chef_solo do |chef|
      chef.roles_path = "roles"
      chef.data_bags_path = "data_bags"
      chef.add_role "Vertica"
    end
  end

  config.vm.define "vertica3" do |vertica|
    vertica.vm.hostname = 'vertica3.backuptest.com'
    vertica.vm.network :private_network, ip: "192.168.10.3"
    vertica.vm.provision :chef_solo do |chef|
      chef.roles_path = "roles"
      chef.data_bags_path = "data_bags"
      chef.add_role "Vertica"
    end
  end

end
