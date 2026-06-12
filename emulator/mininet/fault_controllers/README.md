# Faultynet

Faultynet is a fork of [Containernet](https://github.com/containernet/containernet), which is a fork of the famous [Mininet](http://mininet.org) network emulator.
Faultynet introduces a simple-to-use API that allows for the injection of different simulated network- and host-based
faults.
At this point in time three different fault controller are implemented:
- ConfigFileFaultController, which inserts pre-defined faults into pre-defined links based on a timer
- RandomLinkFaultController, which inserts pre-defined faults into randomly selected links
- MostUsedFaultController, which inserts pre-defined faults into the busiest links

For more details on how to use these fault controller see the [FaultControllersREADME](FaultyControllers.md) file.
For more implementation details see the [Documentation](Faultynet-Documentation.md) file.

## Installation
Faultynet requires `libcgroup1` to be installed on your system. This package is (as of time of writing) not present in apt for ubuntu 24.04, but you can still find .deb files on the internet to install.

Faultynet also needs legacy cgroups. You need to add `SYSTEMD_CGROUP_ENABLE_LEGACY_FORCE=1` and `systemd.unified_cgroup_hierarchy=0` to your kernel parameters and reboot. 

If you are using GRUB, you can add this parameters by appending them to `GRUB_CMDLINE_LINUX_DEFAULT` and `GRUB_CMDLINE_LINUX` in `/etc/default/grub` and running `sudo grub-mkconfig -o /boot/grub/grub.cfg`
```bash
sudo vim /etc/default/grub
```
Add the systemd.unified_cgroup_hierarchy=0 parameter inside the quotes
```bash
GRUB_CMDLINE_LINUX_DEFAULT="systemd.unified_cgroup_hierarchy=0"
GRUB_CMDLINE_LINUX="systemd.unified_cgroup_hierarchy=0"
```
Update GRUB to apply the changes:
```bash
sudo grub-mkconfig -o /boot/grub/grub.cfg
```
Reboot your system to apply. 

## Running a basic example


Make sure you are in the `faultynet` directory. You can start a simple example topology with some nodes and some faults
through one of the provided examples:

```bash
sudo python3 examples/faultinjector_examples/traffic_with_loss_example.py
```
This example, as well as the other ones, launch a net, inject faults into that net, and then shutdown. This process
mirrors how Faultynet might be used in a CI pipeline.
Each test case comes with a .yml config file which shares its name. To modify the faults injected in the `traffic_with_loss.py` example
modify the `traffic_with_loss.yml` file in the same folder.

Currently, all examples use the only implemented FaultController, `ConfigFileFaultController`, which was designed for
repeatable usage in testing pipelines and offers limited interactivity. This fault controller is automatically started
when the corresponding net ist started, and faults are activated and deactivated based on a timer.
For more interactivity, Mininets `CLI()` method can be called and used normally, but due to the timer-based fault scheduling of
`ConfigFileFaultController` this comes with inherent limitations.

For more details about available controllers, see [FaultControllersREADME.md]([FaultControllersREADME.md]).


## Hello World
Using Faultynet is very similar to using Containernet or Mininet. If you're inexperienced wtih Mininet I recommend the
[official documentation](https://github.com/mininet/mininet/wiki/Introduction-to-Mininet) as a starting point. 

Get started for generating your own faulty networks by creating a yml file with the following contents:
```yaml
---
faults:
  - link_fault:
      type: "link_fault:loss"
      pattern_args: [50]
      identifiers:
        - "s1->h1"
        - "h1->s1"
      pattern: "random"
```
Create a net with a node and a switch, and reference the yml config file:
```python
class SimplestTopo(Topo):
    # pylint: disable=arguments-differ
    def build(self):
        h1 = self.addHost('h1')
        switch = self.addSwitch('s1')
        self.addLink(h1, switch)
fault_filepath = "path/to/file.yml"
net = Mininet(topo=SimplestTopo(), waitConnected=True, faultFilepath=fault_filepath)
net.start()
```
You now have a net with 50% packet loss for the traffic between host h1 and switch s1.
## Overview
### Features
Beyond Mininet's and Containernet's features, Faultynet allows you to
- Define and inject faults into arbitrary interfaces
  - in different patterns, including burst, increasing degradation, random, and persistent
  - for different fault types, including data rate limit, delay, loss, corruption, duplication, reordering, and redirection
- Define and inject faults into arbitrary nodes
  - In different patterns, including burst, increasing degradation, and persistent
  - for cpu stressing, and user defined custom faults
- log to files, in user-defined intervals, with user-defined logging commands
- Define custom FaultControllers by only implementing log-parsing and fault-scheduling logic


### Limitations
- Only one fault can be injected per interface at the same time
- Faults can't be injected on bandwidth limited links, or otherwise limited links
- The fault controllers currently don't support nodes that were added during runtime
- The `link_fault:down` and MostUsedFaultController require interfaces to be managed by ifconfig
- When running docker containers the systemd driver is used
  - Modifying this is straightforward, but be aware
- The operating system must use cgroup1 instead of cgroup2
  - A guide on how to set this for an up-to-date Ubuntu is provided below
- FaultyNet currently has no protections against outside tampering on interfaces
  - Most notably, setting tcpdump to listen on an interface will delete nc filters, which disables the redirection fault
- The minimum burst size for cpu stressing is 1 second

### Planned Features
- Remove limitations, to allow faults on limited links, multiple faults per interface, and within docker containers

## Documentation
Faultynets documentation is contained in two files: [FaultControllersREADME](FaultyControllers.md) contains details about
different types of available fault controllers, whereas [Documentation](Faultynet-Documentation.md) contains general information about Faultynet.
Containernet's documentation can be found in the [GitHub wiki](https://github.com/containernet/containernet/wiki).
Faultynets additions are also discussed in a [masters thesis](hand-in-version.pdf), which is contained in this repo for ease of access.
The documentation for the underlying Mininet project can be found on the [Mininet website](http://mininet.org/).

### Support

If you have any questions, please use GitHub's [issue system](https://github.com/ADimeo/Faultynet/issues), or shoot me an email at antonio.dimeo@student.hpi.de

### Contribute

Your contributions are very welcome! Please fork the GitHub repository and create a pull request.
