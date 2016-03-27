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

    # use vagrant-cachier if available (common package cache)
    # (install with "vagrant plugin install vagrant-cachier")
    if Vagrant.has_plugin?("vagrant-cachier")
        # Configure cached packages to be shared between instances of the same base box.
        # More info on the "Usage" link above
        config.cache.scope = :box
    end

end
