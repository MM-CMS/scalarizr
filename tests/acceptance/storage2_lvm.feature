Feature: lvm storage

    Scenario: Extend volume when new disks added
        Given I have LVM layout on top of loop device
         When I extend pvs with another loop
         Then I see volume growth

    Scenario: Cow deleted when lvm snapshot destroyed
    	Given I have LVM layout on top of loop device
    	 When I create and then delete LVM snapshot
    	 Then I do not see cow device file
