Feature: Storage2 volume grow

	Scenario Outline: Grow mounted volume

		Given I have <type> volume with <cfg> settings
		And I create some file on it
		When I grow volume with <growth>
		Then I see that volume grew properly
		And I still see my precious file
   		And unnecessary artifacts were destroyed

   		When I destroy growed volume
   		Then all artifacts were destroyed

   		When I attach my original volume back
   		Then I still see my precious file

		When I grow volume with <growth> and it fails
		Then I see my original volume back
		And I still see my lovely file
		And all artifacts were destroyed


	Examples:
		| type | cfg                    										   | growth |
											
		| loop | size=0.05, fstype=ext3 										   | size=0.1 |
		| loop | size=0.05, fstype=ext4 										   | size=0.1 |
		| loop | size=0.05, fstype=xfs  										   | size=0.1 |
											
#		| ebs  | size=1, fstype=ext3   											   | size=2   |
#		| ebs  | size=1, fstype=ext4    										   | size=2   |
#		| ebs  | size=1, fstype=xfs      										   | size=2   |

		| raid | vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=ext3 | disks.size=0.1        |
		| raid | vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=ext4 | disks.size=0.1        |
		| raid | vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=xfs  | disks.size=0.1        |

		| raid | vg=test,level=1,disks=3,disk.type=loop,disk.size=0.05,fstype=ext3 | disks.size=0.1        |
		| raid | vg=test,level=1,disks=3,disk.type=loop,disk.size=0.05,fstype=ext4 | disks.size=0.1        |
		| raid | vg=test,level=1,disks=3,disk.type=loop,disk.size=0.05,fstype=xfs  | disks.size=0.1        |

		| raid | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3 | disks.size=0.1,disks_count=3  |
		| raid | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4 | disks.size=0.1,disks_count=3  |
		| raid | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs  | disks.size=0.1,disks_count=3  |

		| raid | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3 | disks.size=0.1,disks_count=3  |
		| raid | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4 | disks.size=0.1,disks_count=3  |
		| raid | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs  | disks.size=0.1,disks_count=3  |


	Scenario: grow and change EBS

		Given I have ebs volume with size=1 settings
		And I create some file on it

		When I grow volume with volume_type=io1,iops=100,size=10
		Then I still see my sweet file
		And I see that EBS settings were really changed

