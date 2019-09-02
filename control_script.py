#! /usr/bin/env python2.7

# This is script is for spinning up new AWS instances and controlling their running

import boto.ec2, sys, os, MySQLdb, time

conn = boto.ec2.connect_to_region("<AWS_REGION>",aws_access_key_id='<AWS_ACCESS_KEY>',aws_secret_access_key='<AWS_SECRET_ACCESS_KEY>')

flag = 1
terminated = 0
number_to_terminate=0
max_new_instances=2

reservations = conn.get_all_instances()
print "The initial state of the instances is: "
for r in reservations:
  for i in r.instances:
    print str(r.id)+", "+str(i.id)+", "+str(i.state)
print "\n"

while(flag):

  # The cursor needs to be refreshed each time
  # Get the number of workunits in the queue
  db = MySQLdb.connect("localhost","<DATABASE_USER>","<DATABASE_PASSWORD>","<DATABASE_NAME>" )
  cursor1 = db.cursor()
  query1 = 'select count(id) from result where server_state = 2'
  cursor1.execute(query1)
  queue_count = cursor1.fetchone()
  cursor1.close()
  print "Number of workunits in queue: "+str(queue_count[0])

  # Get the number of workunits running
  cursor2 = db.cursor()
  query2 = 'select count(id) from result where server_state = 4'
  cursor2.execute(query2)
  running_count = cursor2.fetchone()
  cursor2.close()
  print "Number of workunits running: "+str(running_count[0])

  reservations = conn.get_all_instances()
  # Count the number of running or pending instances
  number_currently_running=0
  for r in reservations:
    for i in r.instances:
      if i.state == "running" or i.state == "pending":
        number_currently_running=number_currently_running+1

  print "Number of instances currently running "+str(number_currently_running)
  print "Maximum number of new instances to start "+str(max_new_instances)

  # If there is work in the queue and the number of instances running is less than the maximum
  if (queue_count[0]-running_count[0])>0 and number_currently_running < max_new_instances: #Note, there is a limit of 5 instances
    # If the work in the queue is more than the number of running instances
    if (queue_count[0]-running_count[0]) > number_currently_running:
      # And the difference is less than the maximum number of instances
      if ((queue_count[0]-running_count[0])-number_currently_running) < max_new_instances:
        # Then start instances
        for s in range(1,((queue_count[0]-running_count[0])-number_currently_running)):
          print "Starting an instance"
          conn.run_instances(image_id='<AWS_IMAGE_NAME>',
                             key_name='<AWS_KEY_NAME>',
                             instance_type='m4.2xlarge',
                             subnet_id='<SUBNET_ID>')
          number_to_terminate = number_to_terminate+1
          time.sleep(20) #Wait for status to change
      elif ((queue_count[0]-running_count[0])-number_currently_running) >= max_new_instances:
        # Or start instances to the maximum number of instances
        for s in range(1,max_new_instances):
          print "Starting an instance"
          conn.run_instances(image_id='<AWS_IMAGE_NAME>',
                             key_name='<AWS_KEY_NAME>',
                             instance_type='m4.2xlarge',
                             subnet_id='<SUBNET_ID>')
          number_to_terminate = number_to_terminate+1
          time.sleep(20) #Wait for status to change

  reservations = conn.get_all_instances()
  print "The current state of the instances is: "
  for r in reservations:
    for i in r.instances:
      print str(r.id)+", "+str(i.id)+", "+str(i.state)

      #Stop and terminate instance if nothing running and queue empty
      if queue_count[0] == 0 and running_count[0] == 0:
        if str(i.state) == "running":
          print "Stopping instance: "+i.id
          conn.stop_instances(instance_ids=[i.id])
          time.sleep(30) #Wait for status to change
        if str(i.state) == "stopped":
          print "Terminating instance: "+i.id
          conn.terminate_instances(instance_ids=[i.id])
          time.sleep(30) #Wait for status to change
        if str(i.state) == "terminated":
          terminated = terminated+1

    # Note, this script will also stop those instances already running
    if (queue_count[0]==0 and running_count[0]==0) and (number_to_terminate>=terminated or number_to_terminate==0):
      flag = 0

print "\n"
reservations = conn.get_all_instances()
print "The final state of the instances is: "
for r in reservations:
  for i in r.instances:
    print str(r.id)+", "+str(i.id)+", "+str(i.state)
