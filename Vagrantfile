# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define "deb" do |deb|
    deb.vm.box = "ubuntu/trusty64"
    deb.vm.provision "shell", path: "vagrant/deb.sh"
  end


  # Optional: forward ports from VM to native machine so you can run the streaming job locally; uncomment
  # the next three lines if you want this

  # config.vm.network "forwarded_port", guest: 2181, host: 2181
  # config.vm.network "forwarded_port", guest: 9092, host: 9092
  # config.vm.network "forwarded_port", guest: 8081, host: 8081

  # not sure if this is needed; experimenting with commenting this out
  # config.vm.synced_folder "~/.gnupg", "/root/.gnupg", owner: "root", group: "root"

  config.vm.provider "virtualbox" do |vb|
    # change to 4096 if you're running hive
    vb.customize ["modifyvm", :id, "--memory", "3072"]
  end

  # copy scripts to remote machine
  config.vm.provision "file", source: "vagrant/profile", destination: "/home/vagrant/.profile"
  # config.vm.provision "file", source: "vagrant/setup.sh", destination: "/home/vagrant/setup.sh"
  # config.vm.provision "file", source: "vagrant/start.sh", destination: "/home/vagrant/start.sh"
  # config.vm.provision "file", source: "vagrant/start_hive.sh", destination: "/home/vagrant/start_hive.sh"
  # config.vm.provision "file", source: "vagrant/clean_up.sh", destination: "/home/vagrant/clean_up.sh"

  # copy
  config.vm.provision "file", source: "scripts", destination: "/home/vagrant/scripts"
  config.vm.provision "file", source: "src", destination: "/home/vagrant/src"
  # config.vm.provision "file", source: "config", destination: "/home/vagrant/config"
  config.vm.provision "file", source: "etc", destination: "/home/vagrant/etc"
  config.vm.provision "file", source: "pom.xml", destination: "/home/vagrant/pom.xml"
  config.vm.provision "file", source: "data", destination: "/home/vagrant/data"

    # use vagrant-cachier if available (common package cache)
    # (install with "vagrant plugin install vagrant-cachier")
    if Vagrant.has_plugin?("vagrant-cachier")
        # Configure cached packages to be shared between instances of the same base box.
        # More info on the "Usage" link above
        config.cache.scope = :box
    end

end
