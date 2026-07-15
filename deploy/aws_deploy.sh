#!/bin/bash
# Deploy the kalshi-mm trading bot to AWS us-east-2 as a Docker container.
#
# The repo is shipped as-is and the image is built ON the instance (inside
# rust:trixie via the Dockerfile) — a native on-instance `cargo build` fails
# because Amazon Linux 2023's glibc 2.34 can't link the prebuilt ONNX Runtime
# (needs glibc >= 2.38); see user_data.sh. Runtime secrets (.env + the RSA key
# file it references) are MOUNTED into the container, never baked into the image.
#
# Usage: deploy/aws_deploy.sh [config-name]   (default: prod = KXBTCD + KXWCGAME)

set -e

REGION="us-east-2"
INSTANCE_TYPE="t3.small"
AMI_ID="ami-0772d6acfbccb1275"   # Amazon Linux 2023 in us-east-2
KEY_NAME="kalshi-trading-bot"
SECURITY_GROUP="kalshi-bot-sg"
ROOT_GB=30                        # 8G default can't hold rust:trixie + build + swap
CONFIG="${1:-prod}"               # config/<name>.toml

echo "=========================================="
echo "Deploying kalshi-mm to AWS"
echo "Region: $REGION | Config: $CONFIG"
echo "Mode: LIVE TRADING (REAL MONEY)"
echo "=========================================="
echo

if ! aws sts get-caller-identity &>/dev/null; then
    echo "ERROR: AWS credentials not configured — run: aws configure"
    exit 1
fi

cd "$(dirname "$0")/.."   # repo root

if [ ! -f .env ]; then
    echo "ERROR: .env not found — copy .env.example, fill in live credentials"
    echo "(PAPER_MODE=false), and re-run."
    exit 1
fi
if [ ! -f "config/${CONFIG}.toml" ]; then
    echo "ERROR: config/${CONFIG}.toml not found."
    exit 1
fi
if [ -z "$(ls models/*.onnx 2>/dev/null)" ]; then
    echo "ERROR: models/ has no ONNX checkpoints — copy the deployment"
    echo "categories' *_final.onnx files there first."
    exit 1
fi

# The RSA private-key file that .env's KALSHI_API_SECRET points at must ship too
# (it is gitignored and NOT in the image). Validate it exists locally.
SECRET_REF=$(grep -E '^KALSHI_API_SECRET=' .env | head -1 | cut -d= -f2- | tr -d '"' | xargs)
if [ -n "$SECRET_REF" ] && [[ "$SECRET_REF" != *"BEGIN"* ]] && [[ "$SECRET_REF" != /* ]]; then
    if [ ! -f "$SECRET_REF" ]; then
        echo "ERROR: KALSHI_API_SECRET points at '$SECRET_REF' but that file is"
        echo "missing from the repo root — it must be present to ship to the instance."
        exit 1
    fi
fi

# Security group.
if ! aws ec2 describe-security-groups --region $REGION --group-names $SECURITY_GROUP &>/dev/null; then
    echo "Creating security group: $SECURITY_GROUP"
    SG_ID=$(aws ec2 create-security-group \
        --region $REGION --group-name $SECURITY_GROUP \
        --description "Kalshi Trading Bot Security Group" \
        --query 'GroupId' --output text)
    # NOTE: SSH open to the world for convenience. Tighten to your IP for prod:
    #   --cidr $(curl -s https://checkip.amazonaws.com)/32
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
    --exclude=./target --exclude=./tests --exclude=./.git \
    -C "$(pwd)" .
echo "Package: /tmp/kalshi-mm-deploy.tar.gz ($(du -h /tmp/kalshi-mm-deploy.tar.gz | cut -f1))"

echo
echo "Launching EC2 instance ($INSTANCE_TYPE, ${ROOT_GB}G root, $REGION)..."
INSTANCE_ID=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --key-name $KEY_NAME --security-groups $SECURITY_GROUP \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=${ROOT_GB},VolumeType=gp3}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kalshi-mm}]" \
    --user-data file://deploy/user_data.sh \
    --query 'Instances[0].InstanceId' --output text)
echo "Instance launched: $INSTANCE_ID"

aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_ID
PUBLIC_IP=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Instance running at: $PUBLIC_IP"

SSH="ssh -i $HOME/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no -o ConnectTimeout=15"
echo "Waiting for Docker (installed by user-data) to be ready..."
for i in $(seq 1 40); do
    if $SSH ec2-user@${PUBLIC_IP} 'sudo systemctl is-active docker' 2>/dev/null | grep -q active; then
        echo "Docker is up."
        break
    fi
    sleep 10
done

echo
echo "Shipping code + building image + starting container (on-instance)..."
scp -i $HOME/.ssh/${KEY_NAME}.pem -o StrictHostKeyChecking=no \
    /tmp/kalshi-mm-deploy.tar.gz ec2-user@${PUBLIC_IP}:~/

# bash -s -- "$CONFIG" makes $1 inside the (quoted) heredoc the config name.
$SSH ec2-user@${PUBLIC_IP} "bash -s -- '$CONFIG'" << 'EOF'
set -e
CONFIG="$1"
mkdir -p kalshi-mm && tar xzf kalshi-mm-deploy.tar.gz -C kalshi-mm && rm kalshi-mm-deploy.tar.gz
cd kalshi-mm

# 2G RAM is tight for the ONNX C++ link — add 2G swap if none present.
if ! swapon --show | grep -q .; then
    sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
    sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
fi

# Force live mode regardless of what's in the shipped .env.
sed -i 's/PAPER_MODE=true/PAPER_MODE=false/' .env

# Mount the RSA key file that .env references (skip if inline PEM).
SECRET_REF=$(grep -E '^KALSHI_API_SECRET=' .env | head -1 | cut -d= -f2- | tr -d '"' | xargs)
SECRET_MOUNT=""
if [ -n "$SECRET_REF" ] && [[ "$SECRET_REF" != *"BEGIN"* ]]; then
    if [[ "$SECRET_REF" = /* ]]; then
        SECRET_MOUNT="-v $SECRET_REF:$SECRET_REF:ro"
    elif [ -f "$SECRET_REF" ]; then
        SECRET_MOUNT="-v $PWD/$SECRET_REF:/app/$SECRET_REF:ro"
    fi
fi

sudo docker build -t "kalshi-mm:$CONFIG" .
sudo docker rm -f kalshi 2>/dev/null || true
sudo docker run -d --name kalshi --restart unless-stopped \
    -v "$PWD/.env:/app/.env:ro" $SECRET_MOUNT \
    "kalshi-mm:$CONFIG" --config "$CONFIG"

echo "Container started:"
sudo docker ps --filter name=kalshi
EOF

echo
echo "=========================================="
echo "DEPLOYMENT COMPLETE"
echo "Instance ID: $INSTANCE_ID | Public IP: $PUBLIC_IP | Region: $REGION | Config: $CONFIG"
echo
echo "Logs:      ssh -i ~/.ssh/${KEY_NAME}.pem ec2-user@${PUBLIC_IP} 'sudo docker logs -f kalshi'"
echo "Stop:      ssh -i ~/.ssh/${KEY_NAME}.pem ec2-user@${PUBLIC_IP} 'sudo docker stop kalshi'"
echo "Terminate: aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo
echo "WARNING: LIVE TRADING WITH REAL MONEY IS NOW ACTIVE"
echo "=========================================="