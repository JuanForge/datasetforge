sudo env "PATH=$PATH" py-spy record --subprocesses --idle --rate 100 -o profile.svg -- datasetforge duplicates     --input-cache <>     --top-k 5000

viztracer --min_duration 10 --log_multiprocess -o result.json datasetforge duplicates --input-cache <> --top-k 200
vizviewer result.json

pip install scalene==2.2.1
scalene run     --profile-all     -m datasetforge     duplicates     --input-cache <>     --top-k 500
scalene view scalene-profile.json