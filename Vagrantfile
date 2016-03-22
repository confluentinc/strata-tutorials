# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define "deb" do |deb|
    deb.vm.box = "ubuntu/trusty64"
    deb.vm.provision "shell", path: "vagrant/deb.sh"
  end

  config.vm.synced_folder "~/.gnupg", "/root/.gnupg", owner: "root", group: "root"

  config.vm.provider "virtualbox" do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
  end

    # use vagrant-cachier if available (common package cache)
    # (install with "vagrant plugin install vagrant-cachier")
    if Vagrant.has_plugin?("vagrant-cachier")
        # Configure cached packages to be shared between instances of the same base box.
        # More info on the "Usage" link above
        config.cache.scope = :box
    end

end
