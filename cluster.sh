gcloud beta container --project "toixotoixo" clusters create "gutcluster" --zone "europe-west1-b" --no-enable-basic-auth --cluster-version "1.12.7-gke.10" --machine-type "n1-highmem-4" --image-type "COS" --disk-type "pd-standard" --disk-size "100" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --num-nodes "2" --enable-cloud-logging --enable-cloud-monitoring --no-enable-ip-alias --network "projects/toixotoixo/global/networks/default" --subnetwork "projects/toixotoixo/regions/europe-west1/subnetworks/default" --addons HorizontalPodAutoscaling,HttpLoadBalancing --enable-autoupgrade --enable-autorepair


kubectl create secret generic agrius-gut-keys --from-file keys/
