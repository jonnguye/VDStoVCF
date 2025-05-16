import hail as hl
import argparse
import os

def write_vcf(inputs):
    #LOAD TABLES AND FIND SUBSET
    vds = hl.vds.read_vds(inputs['vds'])
    samples_table = hl.import_table(inputs['samples_list'], no_header=True)
    samples_table = samples_table.rename({'f0': 's'})
    samples_table = samples_table.key_by('s')

    vds = hl.vds.filter_samples(vds, samples_table, keep=True)

    print(f"Filtering to {mt.count_cols()} samples")

    #SELECT WHICH CHROMOSOME TO FILTER BY

    if inputs['chr'] is None or inputs['chr'].upper() == 'ALL':
        print("NO CHR FILTER APPLIED")
        print(f"CHR value provided: {inputs['chr']}")
    else:
        print(f"Filtering on {inputs['chr']}")
        vds = hl.vds_filter_chromosomes(vds, keep=inputs['chr'])

    #filter for biallelic
    vds = vds.filter_rows(vds.alleles.length() == 2)

    mt = hl.vds.to_dense_mt(vds, reference_genome='GRCh38')
    hl.export_vcf(mt, f"{inputs['output_prefix']}.vcf.bgz")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix_table", required=True)
    parser.add_argument("--samples_list", required=True)
    parser.add_argument("--chr", required=True)
    parser.add_argument("--output_prefix", required=True)

    args = parser.parse_args()

    inputs = {
        'matrix_table': args.matrix_table,
        'samples_list': args.samples_list,
        'chr': args.chr,
        'output_prefix': args.output_prefix,
    }

    hl.init(
        app_name='hail_job',
        master='local[*]',
        tmp_dir='gs://fc-secure-b8771cfd-5455-4292-a720-8533eb501a93/hail-tmp/',
        spark_conf={
            'spark.executor.instances': '4',
            'spark.executor.cores': '8',
            'spark.executor.memory': '25g',
            'spark.driver.memory': '30g',
            'spark.local.dir': '/cromwell_root',
            'spark.sql.shuffle.partitions': '100',
            'spark.default.parallelism': '100',
            'spark.memory.fraction': '0.8',
            'spark.memory.storageFraction': '0.2',
        },
        default_reference='GRCh38'
    )
    
    print("Spark local directories:", os.getenv("SPARK_LOCAL_DIRS"), flush=True)
    print("Disk usage:", flush=True)
    os.system("df -h")
    
    write_vcf(inputs)
