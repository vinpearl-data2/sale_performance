set -e
. /mnt/miniconda3/etc/profile.d/conda.sh
conda activate dbt2
cd /home/liendtm/workplace/git_hub/sale_performance/dbt
git pull
dbt run --profiles-dir . --target prod --models dwh.SALE_PERFORMANCE.revenue_tracking