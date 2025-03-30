job_cluster_spec = [
    {
        "job_cluster_key": "market-pulse-job-cluster",
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "16.2.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "auto",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "m5d.large",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "DATA_SECURITY_MODE_AUTO",
            "kind": "CLASSIC_PREVIEW",
            "runtime_engine": "PHOTON",
            "num_workers": 1,
        },
    }
]

job_cluster_ml_spec = [
    {
        "job_cluster_key": "market-pulse-job-cluster-ml",
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "16.2.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "auto",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "m5d.large",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "DATA_SECURITY_MODE_AUTO",
            "kind": "CLASSIC_PREVIEW",
            "runtime_engine": "PHOTON",
            "num_workers": 1,
            "use_ml_runtime": True
        },
    }
]