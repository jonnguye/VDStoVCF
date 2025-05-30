version 1.0

workflow WriteVCFWorkflow {
    input {
        String vds
        String samples_list
        String chr = "ALL"
        String output_prefix
    }

    call WriteVCFTask {
        input:
            vds = vds,
            samples_list = samples_list,
            chr = chr,
            output_prefix = output_prefix
    }

    call IndexVCF {
        input:
            vcf_file = WriteVCFTask.output_vcf
    }


    output {
        File output_vcf = WriteVCFTask.output_vcf
        File output_vcf_index = IndexVCF.vcf_index
    }
}

task WriteVCFTask {
    input {
        String vds
        String samples_list
        String chr
        String output_prefix
    }

    command <<<
        set -e

        export SPARK_LOCAL_DIRS=/cromwell_root

        echo "Checking disk mounts and usage:"
        df -h
        echo "Checking Spark local directory:"
        echo $SPARK_LOCAL_DIRS
        echo "Checking /cromwell_root directory:"
        ls -lah /cromwell_root

        curl -O https://raw.githubusercontent.com/jonnguye/VDStoVCF/main/write_vcf.py

        python3 write_vcf.py \
            --vds "~{vds}" \
            --samples_list "~{samples_list}" \
            --chr "~{chr}" \
            --output_prefix "~{output_prefix}" \
    >>>

    runtime {
        docker: "quay.io/jonnguye/hail:latest"
        memory: "256G"
        cpu: 64
        disks: "local-disk 1000 SSD"
    }

    output {
        File output_vcf = "~{output_prefix}.vcf.bgz"
    }
}

task IndexVCF {
    input {
        File vcf_file
    }

    command <<<
        bcftools index -c --threads 4 "~{vcf_file}"
        >>>
    
    runtime {
        docker: "quay.io/biocontainers/bcftools:1.21--h3a4d415_1"
        memory: "32G"
        cpu: 4
        disks: "local-disk 100 SSD"
    }

    output {
        File vcf_index = "~{vcf_file}.csi"
    }
}
