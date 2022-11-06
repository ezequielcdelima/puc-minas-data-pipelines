eksctl create cluster \
    --name=kubece \
    --managed \
    --instance-types=m5.xlarge \
    --alb-ingress-access -- node-private-networking \
    --region=us-east-1 \
    --nodes-min=2 --nodes-max=3 \
    --full-ecr-access \
    --asg-access \
    --nodegroup-name=ng-kubece


eksctl create cluster --name=kubece --managed --instance-types=m5.xlarge --alb-ingress-access --node-private-networking --region=us-east-1 --nodes-min=2 --nodes-max=3 --full-ecr-access --asg-access --nodegroup-name=ng-kubece
