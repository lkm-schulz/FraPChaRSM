#!/bin/bash

KUBE_NAMESPACE="default"
KUBE_ACCOUNT="spark"
KUBE_ROLE="spark-role"

DIR_WORK="work-dir"
SPARK_VER="3.5.1"
SPARK_TYPE="bin-hadoop3"
SPARK_FULL="spark-${SPARK_VER}-${SPARK_TYPE}"

DIR_WAREHOUSE="docker/warehouse"
DIR_DBDATA="docker/db_data"
DIR_MINIODATA="docker/minio_data"
DIR_INFLUXDATA="docker/influx_data"

DIR_DOCKER="docker"
DIR_JARS="${DIR_DOCKER}/jars"

create_dir_if_nonexist()
{
	dirname=$1
	if [[ ! -d "${dirname}" ]]; then
		echo "Creating missing \"${dirname}\" directory..."
		mkdir "${dirname}"
	fi
}

create_replace_dir()
{
	dirname=$1
	if [[ -d "${dirname}" ]]; then 
		read -p "Directory \"${dirname}\" already exists. Delete and replace? [y/N] " selection
		selection=$(echo "${selection}" | tr '[:upper:]' '[:lower:]')

		if [ "${selection}" = "y" ]; then

			echo "Replacing \"${dirname}\"..."
			rm -rf "${dirname}"
			mkdir "${dirname}"
		fi
	else
		echo "Creating \"${dirname}\" directory..."
		mkdir "${dirname}"
	fi
}

kubectl_create()
{
	resource=$1
	name=$2
	namespace=$3
	add_options=$4	

	if kubectl get ${resource} ${name} -n ${namespace} &> /dev/null; then
	    read -p "Resource \"${resource}\" with name \"${name}\" in namespace \"${namespace}\" already exists. \
Delete and replace? [y/N] " selection

		selection=$(echo "${selection}" | tr '[:upper:]' '[:lower:]')

		if [ "${selection}" = "y" ]; then
			kubectl delete ${resource} ${name} -n ${namespace}
			kubectl create ${resource} ${name} -n ${namespace} ${add_options}
		fi
	    
	else
		kubectl create ${resource} ${name} -n ${namespace} ${add_options}
	fi
	
}

echo "Installing docker..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-compose
sudo groupadd docker
sudo usermod -aG docker $USER
sudo systemctl enable docker.service
sudo systemctl enable containerd.service

echo "Installing zip and unzip..."
sudo apt install -y zip unzip

if [[ ! -d "$HOME/.sdkman" ]]; then
    echo "SDKMAN! is not installed. Installing..."
    curl -s "https://get.sdkman.io/" | bash
else
    echo "SDKMAN! is already installed."
fi

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 11.0.22-tem
sdk install sbt

echo "Creating folders for Hive store..."
create_dir_if_nonexist ${DIR_DOCKER}

create_replace_dir ${DIR_WAREHOUSE}
create_replace_dir ${DIR_DBDATA}
create_replace_dir ${DIR_MINIODATA}
create_replace_dir ${DIR_INFLUXDATA}

echo "Downloading dependency jar files..."
create_dir_if_nonexist ${DIR_JARS}

wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P ${DIR_JARS}
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P ${DIR_JARS}
wget https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.5.1/spark-hadoop-cloud_2.12-3.5.1.jar -P ${DIR_JARS}

create_replace_dir ${DIR_WORK}

cd "${DIR_WORK}"
echo "Downloading Minio client..."
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

echo "Downloading Spark (${SPARK_VER})..."
wget "https://dlcdn.apache.org/spark/spark-${SPARK_VER}/${SPARK_FULL}.tgz"
echo "Unpacking Spark..."
tar xf "${SPARK_FULL}.tgz"
rm "${SPARK_FULL}".tgz

echo "Copying Spark config..."
cd ..
cp spark-defaults.conf "${DIR_WORK}/${SPARK_FULL}/conf/"

echo "Creating K8s service account for Spark..."

if [ "${KUBE_NAMESPACE}" != "default" ]; then
	kubectl_create namespace ${KUBE_NAMESPACE} ${KUBE_NAMESPACE}
fi
kubectl_create serviceaccount ${KUBE_ACCOUNT} ${KUBE_NAMESPACE}
kubectl_create rolebinding ${KUBE_ROLE} ${KUBE_NAMESPACE} "--clusterrole=edit --serviceaccount=${KUBE_NAMESPACE}:${KUBE_ACCOUNT}"

