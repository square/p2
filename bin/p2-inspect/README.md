# p2-inspect

`p2-inspect` is a tool for examining the state of a p2 cluster. Given the address of any consul agent, it will pull the manifest SHAs and health checks for all the pods running in the cluster, across all its nodes. Example:

```bash
$ p2-inspect | python -mjson.tool
```

```json
{
    "isup": {
        "aws1.example.com": {
            "health_check": {
                "output": "[/data/pods/isup/isup/installs/isup_vsjlzlxvnkizuxmkutqmkqhwukyryztuxhusnkpm/bin/launch]\n[PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin:/bin TERM=linux RUNLEVEL=3 PREVLEVEL=N UPSTART_EVENTS=runlevel UPSTART_JOB=runit UPSTART_INSTANCE= CONFIG_PATH=/data/pods/isup/config/isup_717cc0d58df240e2668c865cdc063d715446e6db.yaml]\n",
                "status": "passing"
            },
            "intent_manifest_sha": "717cc0d58df240e2668c865cdc063d715446e6db",
            "reality_manifest_sha": "717cc0d58df240e2668c865cdc063d715446e6db"
        },
        "aws2.example.com": {
            "health_check": {
                "output": "[/data/pods/isup/isup/installs/isup_tkkhnurngovsomvzikznymgmluohzjvniwzrtpxq/bin/launch]\n[PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin:/bin TERM=linux RUNLEVEL=3 PREVLEVEL=N UPSTART_EVENTS=runlevel UPSTART_JOB=runit UPSTART_INSTANCE= CONFIG_PATH=/data/pods/isup/config/isup_b56d3c3fd3c264841c8aad6a9ce6f06271a62dc6.yaml]\n",
                "status": "passing"
            },
            "intent_manifest_sha": "b56d3c3fd3c264841c8aad6a9ce6f06271a62dc6",
            "reality_manifest_sha": "b56d3c3fd3c264841c8aad6a9ce6f06271a62dc6"
        }
    },
    "p2-preparer": {
        "aws1.example.com": {
            "intent_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d",
            "reality_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d"
        },
        "aws2.example.com": {
            "intent_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d",
            "reality_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d"
        },
        "aws3.example.com": {
            "intent_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d",
            "reality_manifest_sha": "9b9c7cb38b9a68564d6d582c05298cd3dab02e9d"
        }
    }
}
```

This indicates that there are three nodes running the `p2-preparer` pod. This pod has no health check; hence there is no `health_check` object in the JSON. Meanwhile, there are also two instances of the `isup` pod. These pods both have passing health checks, and their health check scripts produced some output that you can see in the JSON above.

You can filter by pod ID, by node name, or by both:

```bash
$ p2-inspect --node aws1.example.com --pod isup | python -m json.tool
```

```json
{
    "isup": {
        "aws1.example.com": {
            "health_check": {
                "output": "[/data/pods/isup/isup/installs/isup_vsjlzlxvnkizuxmkutqmkqhwukyryztuxhusnkpm/bin/launch]\n[PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin:/bin TERM=linux RUNLEVEL=3 PREVLEVEL=N UPSTART_EVENTS=runlevel UPSTART_JOB=runit UPSTART_INSTANCE= CONFIG_PATH=/data/pods/isup/config/isup_717cc0d58df240e2668c865cdc063d715446e6db.yaml]\n",
                "status": "passing"
            },
            "intent_manifest_sha": "717cc0d58df240e2668c865cdc063d715446e6db",
            "reality_manifest_sha": "717cc0d58df240e2668c865cdc063d715446e6db"
        }
    }
}
```
