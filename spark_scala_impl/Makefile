# Build the jar with all it's dependencies
gradle clean
gradle shadowJar

# Build the docker image of the project
docker build -t anomaly-disease .

# Start a local k8s cluster
minikube start

# submit spark job
spark-submit \
    --master k8s://https://192.168.64.3:8443 \
    --deploy-mode cluster \
    --name example \
    --conf spark.executor.instances=5 \
    --class Main \
    --conf spark.kubernetes.container.image=anomaly-disease \
    local:///opt/spark/work-dir/spark-main.jar

# stop the cluster
minikube stop