# delete_cloudformation.py

import boto3
import time

def delete_all_stacks(region='us-east-1', wait=True):
    """
    Deletes all CloudFormation stacks with statuses 'CREATE_COMPLETE' and 'UPDATE_COMPLETE' in the specified region.

    This function uses the boto3 library to interact with AWS CloudFormation. It lists all stacks
    with the specified statuses, deletes each one, and waits for the deletion to complete if wait is True.

    Parameters:
    region (str): The AWS region to target. Defaults to 'us-east-1'.
    wait (bool): Whether to wait for each stack to be deleted before deleting the next one. Defaults to True.

    Returns:
    None
    """
    # Create a CloudFormation client for the specified region
    client = boto3.client('cloudformation', region_name=region)
    
    # Initialize a paginator to handle pagination
    paginator = client.get_paginator('list_stacks')
    page_iterator = paginator.paginate(StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'ROLLBACK_COMPLETE', 'DELETE_FAILED'])
    
    # First, delete stacks containing 'AppPipe' in their name
    for page in page_iterator:
        for stack in page['StackSummaries']:
            stack_name = stack['StackName']
            if 'AppPipe' in stack_name or 'AppIngestion' in stack_name:
                # Check if the stack has termination protection enabled
                stack_details = client.describe_stacks(StackName=stack_name)
                if stack_details['Stacks'][0]['EnableTerminationProtection']:
                    print(f"Skipping stack: {stack_name} in region {region} due to termination protection")
                    continue
                
                print(f"Deleting stack: {stack_name} in region {region}")
                client.delete_stack(StackName=stack_name)
                if wait:
                    # Optionally, wait for the stack to be deleted
                    waiter = client.get_waiter('stack_delete_complete')
                    waiter.wait(StackName=stack_name)
                    print(f"Deleted stack: {stack_name} in region {region}")
    
    # Reinitialize the paginator to handle pagination again
    page_iterator = paginator.paginate(StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'ROLLBACK_COMPLETE'])
    
    # Then, delete all other stacks
    for page in page_iterator:
        for stack in page['StackSummaries']:
            stack_name = stack['StackName']
            if 'AppPipe' not in stack_name:
                # Check if the stack has termination protection enabled
                stack_details = client.describe_stacks(StackName=stack_name)
                if stack_details['Stacks'][0]['EnableTerminationProtection']:
                    print(f"Skipping stack: {stack_name} in region {region} due to termination protection")
                    continue
                
                print(f"Deleting stack: {stack_name} in region {region}")
                client.delete_stack(StackName=stack_name)
                if wait:
                    # Optionally, wait for the stack to be deleted
                    waiter = client.get_waiter('stack_delete_complete')
                    waiter.wait(StackName=stack_name)
                    print(f"Deleted stack: {stack_name} in region {region}")


def delete_all_auto_scaling_groups(region='us-east-1'):
    """
    Deletes all Auto Scaling groups in the specified AWS region.

    This function lists all Auto Scaling groups in the given region and deletes each one.

    Parameters:
    - region (str): The AWS region where the Auto Scaling groups are located. Default is 'us-west-2'.

    Note:
    - Ensure that you have the necessary AWS permissions to delete Auto Scaling groups.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create an Auto Scaling client for the specified region
    client = boto3.client('autoscaling', region_name=region)
    
    # List all Auto Scaling groups in the specified region
    paginator = client.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate()
    
    for page in page_iterator:
        for asg in page['AutoScalingGroups']:
            asg_name = asg['AutoScalingGroupName']
            
            print(f"Deleting Auto Scaling group: {asg_name} in region {region}")
            client.delete_auto_scaling_group(AutoScalingGroupName=asg_name, ForceDelete=True)
            
            print(f"Deleted Auto Scaling group: {asg_name} in region {region}")


def delete_all_lambda_functions(region='us-east-1'):
    """
    Deletes all AWS Lambda functions in the specified AWS region.

    This function lists all Lambda functions in the given region and deletes each one.
    It uses a custom polling mechanism to ensure that each Lambda function is fully deleted before
    proceeding to the next one.

    Parameters:
    - region (str): The AWS region where the Lambda functions are located. Default is 'us-east-1'.

    Note:
    - Ensure that you have the necessary AWS permissions to delete Lambda functions.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create a Lambda client for the specified region
    client = boto3.client('lambda', region_name=region)
    
    # List all Lambda functions in the specified region
    paginator = client.get_paginator('list_functions')
    page_iterator = paginator.paginate()
    
    for page in page_iterator:
        for function in page['Functions']:
            function_name = function['FunctionName']
            
            print(f"Deleting Lambda function: {function_name} in region {region}")
            client.delete_function(FunctionName=function_name)
            
            # Custom polling mechanism to check if the function is deleted
            while True:
                try:
                    client.get_function(FunctionName=function_name)
                except client.exceptions.ResourceNotFoundException:
                    print(f"Deleted Lambda function: {function_name} in region {region}")
                    break
                except Exception as e:
                    print(f"Error checking function status: {e}")
                    break
                time.sleep(5)  # Wait for 5 seconds before checking again


def delete_all_opensearch_clusters(region='us-east-1'):
    """
    Deletes all Amazon OpenSearch clusters in the specified AWS region.

    This function lists all OpenSearch clusters in the given region and deletes each one.
    It uses AWS waiters to ensure that each OpenSearch cluster is fully deleted before
    proceeding to the next one.

    Parameters:
    - region (str): The AWS region where the OpenSearch clusters are located. Default is 'us-west-2'.

    Note:
    - Ensure that you have the necessary AWS permissions to delete OpenSearch clusters.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create an OpenSearch client for the specified region
    client = boto3.client('opensearch', region_name=region)
    
    # List all OpenSearch clusters in the specified region
    clusters = client.list_domain_names()['DomainNames']
    
    for cluster in clusters:
        domain_name = cluster['DomainName']
        
        print(f"Deleting OpenSearch cluster: {domain_name} in region {region}")
        client.delete_domain(DomainName=domain_name)
        print(f"Deleted OpenSearch cluster: {domain_name} in region {region}")


def delete_all_eks_clusters(region='us-east-1', wait=False):
    """
    Deletes all Amazon EKS clusters in the specified AWS region.

    This function lists all EKS clusters in the given region, deletes all node groups
    associated with each cluster, and then deletes the clusters themselves. It uses
    AWS waiters to ensure that node groups and clusters are fully deleted before
    proceeding to the next step.

    Parameters:
    - region (str): The AWS region where the EKS clusters are located. Default is 'ap-northeast-1'.
    - wait (bool): If True, the function will wait for each node group and cluster to be fully
      deleted before proceeding. Default is False.

    Note:
    - Ensure that you have the necessary AWS permissions to delete EKS clusters and node groups.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create an EKS client for the specified region
    client = boto3.client('eks', region_name=region)
    
    # List all EKS clusters in the specified region
    clusters = client.list_clusters()['clusters']
    
    for cluster_name in clusters:
        # List all node groups for the current cluster
        node_groups = client.list_nodegroups(clusterName=cluster_name)['nodegroups']
        
        # Iterate over each node group and delete it
        for node_group in node_groups:
            print(f"Deleting node group: {node_group} in cluster: {cluster_name}")
            client.delete_nodegroup(clusterName=cluster_name, nodegroupName=node_group)
            
            # Wait for the node group to be fully deleted
            waiter = client.get_waiter('nodegroup_deleted')
            waiter.wait(clusterName=cluster_name, nodegroupName=node_group)
        
        # After all node groups are deleted, delete the cluster
        print(f"Deleting EKS cluster: {cluster_name} in region {region}")
        client.delete_cluster(name=cluster_name)
        
        # Optionally wait for the cluster to be fully deleted
        if wait:
            waiter = client.get_waiter('cluster_deleted')
            waiter.wait(name=cluster_name)


def delete_all_peering_connections(region='us-east-1'):
    """
    List all VPC peering connections, delete them, and if there are routes associated with the peering connections, delete them.
    """
    client = boto3.client('ec2', region_name=region)
    response = client.describe_vpc_peering_connections()
    
    for peering_connection in response['VpcPeeringConnections']:
        peering_connection_id = peering_connection['VpcPeeringConnectionId']
        print(f"Deleting VPC peering connection: {peering_connection_id} in region {region}")
        
        # Delete routes associated with the peering connection
        routes = client.describe_route_tables(Filters=[{'Name': 'route.vpc-peering-connection-id', 'Values': [peering_connection_id]}])
        for route_table in routes['RouteTables']:
            for route in route_table['Routes']:
                if route.get('VpcPeeringConnectionId') == peering_connection_id:
                    client.delete_route(RouteTableId=route_table['RouteTableId'], DestinationCidrBlock=route['DestinationCidrBlock'])
                    print(f"Deleted route: {route['DestinationCidrBlock']} in route table: {route_table['RouteTableId']}")
        
        # Delete the peering connection
        client.delete_vpc_peering_connection(VpcPeeringConnectionId=peering_connection_id)
        print(f"Deleted VPC peering connection: {peering_connection_id} in region {region}")


def delete_all_kinesis_streams(region='us-east-1'):
    """
    List all Kinesis data streams and Firehose delivery streams, and delete them.
    """
    kinesis_client = boto3.client('kinesis', region_name=region)
    firehose_client = boto3.client('firehose', region_name=region)
    
    # Delete Kinesis data streams
    data_streams = kinesis_client.list_streams()['StreamNames']
    for stream_name in data_streams:
        print(f"Deleting Kinesis data stream: {stream_name} in region {region}")
        kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
    
    # Delete Kinesis Firehose delivery streams
    firehose_streams = firehose_client.list_delivery_streams()['DeliveryStreamNames']
    for stream_name in firehose_streams:
        print(f"Deleting Kinesis Firehose delivery stream: {stream_name} in region {region}")
        firehose_client.delete_delivery_stream(DeliveryStreamName=stream_name)


def terminate_all_ec2_instances(region='us-east-1', wait=True):
    """
    Terminates all EC2 instances in the specified AWS region.

    This function lists all EC2 instances in the given region and terminates each one.
    It uses AWS waiters to ensure that each EC2 instance is fully terminated before
    proceeding to the next one if wait is True.

    Parameters:
    - region (str): The AWS region where the EC2 instances are located. Default is 'us-east-1'.
    - wait (bool): If True, the function will wait for each EC2 instance to be fully
      terminated before proceeding. Default is True.

    Note:
    - Ensure that you have the necessary AWS permissions to terminate EC2 instances.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create an EC2 client for the specified region
    client = boto3.client('ec2', region_name=region)
    
    # List all EC2 instances in the specified region
    paginator = client.get_paginator('describe_instances')
    page_iterator = paginator.paginate()
    
    for page in page_iterator:
        for reservation in page['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                
                print(f"Terminating EC2 instance: {instance_id} in region {region}")
                client.terminate_instances(InstanceIds=[instance_id])
                
                if wait:
                    # Optionally, wait for the EC2 instance to be fully terminated
                    waiter = client.get_waiter('instance_terminated')
                    waiter.wait(InstanceIds=[instance_id])
                    print(f"Terminated EC2 instance: {instance_id} in region {region}")

def delete_all_ecs_clusters(region='us-east-1', wait=True):
    """
    Deletes all ECS clusters in the specified AWS region.

    This function lists all ECS clusters in the given region and deletes each one.
    Before deleting a cluster, it ensures that all tasks and services within the cluster are deleted.
    It uses AWS waiters to ensure that each ECS cluster is fully deleted before proceeding to the next one if wait is True.

    Parameters:
    - region (str): The AWS region where the ECS clusters are located. Default is 'us-east-1'.
    - wait (bool): If True, the function will wait for each ECS cluster to be fully deleted before proceeding. Default is True.

    Note:
    - Ensure that you have the necessary AWS permissions to delete ECS clusters, services, and tasks.
    - This operation is destructive and cannot be undone. Use with caution.
    """
    # Create an ECS client for the specified region
    client = boto3.client('ecs', region_name=region)
    
    # List all ECS clusters in the specified region
    paginator = client.get_paginator('list_clusters')
    page_iterator = paginator.paginate()
    
    for page in page_iterator:
        for cluster_arn in page['clusterArns']:
            # List and delete all services in the cluster
            service_paginator = client.get_paginator('list_services')
            service_page_iterator = service_paginator.paginate(cluster=cluster_arn)
            for service_page in service_page_iterator:
                for service_arn in service_page['serviceArns']:
                    print(f"Deleting service: {service_arn} in cluster: {cluster_arn} in region {region}")
                    client.update_service(cluster=cluster_arn, service=service_arn, desiredCount=0)
                    client.delete_service(cluster=cluster_arn, service=service_arn, force=True)
                    if wait:
                        waiter = client.get_waiter('services_inactive')
                        waiter.wait(cluster=cluster_arn, services=[service_arn])
                        print(f"Deleted service: {service_arn} in cluster: {cluster_arn} in region {region}")
            
            # List and stop all tasks in the cluster
            task_paginator = client.get_paginator('list_tasks')
            task_page_iterator = task_paginator.paginate(cluster=cluster_arn)
            for task_page in task_page_iterator:
                for task_arn in task_page['taskArns']:
                    print(f"Stopping task: {task_arn} in cluster: {cluster_arn} in region {region}")
                    client.stop_task(cluster=cluster_arn, task=task_arn)
                    if wait:
                        waiter = client.get_waiter('tasks_stopped')
                        waiter.wait(cluster=cluster_arn, tasks=[task_arn])
                        print(f"Stopped task: {task_arn} in cluster: {cluster_arn} in region {region}")
            
            # Delete the ECS cluster
            print(f"Deleting ECS cluster: {cluster_arn} in region {region}")
            client.delete_cluster(cluster=cluster_arn)
            if wait:
                waiter = client.get_waiter('cluster_deleted')
                waiter.wait(clusters=[cluster_arn])
                print(f"Deleted ECS cluster: {cluster_arn} in region {region}")



# Example usage
if __name__ == "__main__":

    # Change the region to the one you want to delete
    region = 'cn-north-1'

    delete_all_auto_scaling_groups(region)
    delete_all_ecs_clusters(region, wait=True)
    terminate_all_ec2_instances(region, wait=False)
    delete_all_lambda_functions(region)
    delete_all_peering_connections(region)
    delete_all_opensearch_clusters(region)
    delete_all_kinesis_streams(region)
    delete_all_eks_clusters(region)

    # after all blocking resources are deleted, delete the cloudformation stacks
    delete_all_stacks(region, wait=False)

    
