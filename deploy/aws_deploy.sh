#!/bin/bash
# Deploy the kalshi-mm trading bot to AWS us-east-2.
# Ships the repo as-is (self-contained: src/, Cargo files, config/, .env,
# models/) and builds it ON the instance — see user_data.sh for why.

set -e

REGION="us-east-2"
INSTANCE_TYPE="t3.small"
AMI_ID="ami-0772d6acfbccb1275"  # Amazon Linux 2023 in us-east-2
KEY_NAME="kalshi-trading-bot"
SECURITY_GROUP="kalshi-bot-sg"

echo "=========================================="
echo "Deploying kalshi-mm to AWS"
echo "Region: $REGION"
echo "Mode: LIVE TRADING (REAL MONEY)"
echo "=========================================="
echo

if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS credentials not configured"
    echo "Run: aws configure"
    exit 1
fi

cd "$(dirname "$0")/.."   # repo root

if [ ! -f .env ]; then
    echo "ERROR: .env not found — copy .env.example, fill in live credentials"
    echo "(PAPER_MODE=false), and re-run."
    exit 1
fi
if [ -z "$(ls models/*.onnx 2>/dev/null)" ]; then
    echo "ERROR: models/ has no ONNX checkpoints — copy the deployment"
    echo "categories' *_final.onnx files there first."
    exit 1
fi

# Security group.
if ! aws ec2 describe-security-groups --region $REGION --group-names $SECURITY_GROUP &>/dev/null; then
    echo "Creating security group: $SECURITY_GROUP"
    SG_ID=$(aws ec2 create-security-group \
        --region $REGION \
        --group-name $SECURITY_GROUP \
        --description "Kalshi Trading Bot Security Group" \
        --query 'GroupId' --output text)
    aws ec2 authorize-security-group-ingress \
        --region $REGION --group-id $SG_ID \
        --protocol tcp --port 22 --cidr 0.0.0.0/0
    echo "Security group created: $SG_ID"
else
    echo "Security group already exists: $SECURITY_GROUP"
fi

# Key pair.
if ! aws ec2 describe-key-pairs --region $REGION --key-names $KEY_NAME &>/dev/null; then
    echo "Creating EC2 key pair: $KEY_NAME"
    aws ec2 create-key-pair \
        --region $REGION --key-name $KEY_NAME \
        --query 'KeyMaterial' --output text > ~/.ssh/${KEY_NAME}.pem
    chmod 400 ~/.ssh/${KEY_NAME}.pem
    echo "Key pair saved to: ~/.ssh/${KEY_NAME}.pem"
else
    echo "Key pair already exists: $KEY_NAME"
fi

echo
echo "Creating deployment package (repo, minus target/ and tests/)..."
tar czf /tmp/kalshi-mm-deploy.tar.gz \
    --exclude=./target \
    --exclude=./tests \
    --exclude=./.git \
    -C "$(pwd)" .
echo "Package: /tmp/kalshi-mm-deploy.tar.gz ($(du -h /tmp/kalshi-mm-deploy.tar.gz | cut -f1))"

echo
echo "Launching EC2 instance ($INSTANCE_TYPE, $REGION)..."
INSTANCE_ID=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --key-name $KEY_NAME --security-groups $SECURITY_GROUP \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kalshi-mm}]" \
    --user-data file://deploy/user_data.sh \
    --query 'Instances[0].InstanceId' --output text)
echo "Instance launched: $INSTANCE_ID"

aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_ID
PUBLIC_IP=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Instance running at: $PUBLIC_IP"
echo "Waiting 90s for rustup install via user-data..."
sleep 90

echo
echo "Deploying code + building (release, on-instance)..."
scp -i ~/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no \
    /tmp/kalshi-mm-deploy.tar.gz ec2-user@${PUBLIC_IP}:~/
ssh -i ~/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no ec2-user@${PUBLIC_IP} << 'EOF'
source $HOME/.cargo/env
mkdir -p kalshi-mm && tar xzf kalshi-mm-deploy.tar.gz -C kalshi-mm
rm kalshi-mm-deploy.tar.gz
cd kalshi-mm
sed -i 's/PAPER_MODE=true/PAPER_MODE=false/' .env
chmod +x start_live_trading.sh
nohup ./start_live_trading.sh > trading.log 2>&1 &
echo "Trading bot started!"
EOF

echo
echo "=========================================="
echo "DEPLOYMENT COMPLETE"
echo "Instance ID: $INSTANCE_ID | Public IP: $PUBLIC_IP | Region: $REGION"
echo
echo "Monitor:   ssh -i ~/.ssh/${KEY_NAME}.pem ec2-user@${PUBLIC_IP}  # then: tail -f kalshi-mm/trading.log"
echo "Stop:      pkill -f kalshi-mm"
echo "Terminate: aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo
echo "WARNING: LIVE TRADING WITH REAL MONEY IS NOW ACTIVE"
echo "=========================================="
