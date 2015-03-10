__author__ = 'CJ'
import os
import unittest
import subprocess
import tempfile
import shutil

from exomes10k.workflow import WorkFlow
from exomes10k.workflow import Step

data="/home/ubuntu/data"
tool_dir = "/home/ubuntu/tools"

memory=15
halfMemory= memory/2
cores=4

uuid='123456789'
normal=uuid + '.N.bam'
tumor=uuid + '.T.bam'

normalBai = normal + '.bai'
tumorBai = tumor + '.bai'

# Specify protected input set
input_set = {}

class PCAWG(WorkFlow):
    def __init__(self):
        """
        naming: step#, description, p= parallel, n = normal, t = tumor steps
        """
        s1_index_p = Step(command="{tool_dir}samtools index {data}/{normal} & samtools index {data}/{tumor} & wait".format(**globals()),
                          inputs={normal, tumor},
                          outputs={normalBai, tumorBai})

        s2_RTC_n = Step(command="java -Xmx{MEM}g -jar GenomeAnalysisTK.jar \
                                -T RealignerTargetCreator \
                                -nt {cores} \
                                -R {data}/genome.fa \
                                -I {data}/{normal} \
                                -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                --downsampling_type NONE \
                                -o {data}/{uuid}.normal.intervals".format(**globals()),
                        inputs={normal, normalBai},
                        outputs={"{data}/{uuid}.normal.intervals".format(**globals())})

        s2_RTC_t = Step(inputs="java -Xmx{MEM}g -jar GenomeAnalysisTK.jar \
                                -T RealignerTargetCreator \
                                -nt {cores} \
                                -R ${data}/genome.fa \
                                -I ${data}/${TUMOUR} \
                                -known ${data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known ${data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                --downsampling_type NONE \
                                -o {data}/{uuid}.tumour.intervals".format(**globals()),
                        inputs={tumor, tumorBai},
                        outputs={"{data}/{uuid}.tumour.intervals".format(**globals())})

        s3_IR_n = Step(command="java -Xmx{HMEM}g -jar GenomeAnalysisTK.jar \
                                -T IndelRealigner \
                                -R {data}/genome.fa \
                                -I {data}/{normal}  \
                                -targetIntervals {data}/{uuid}.normal.intervals \
                                --downsampling_type NONE \
                                -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                -maxReads 720000 -maxInMemory 5400000 \
                                -o {data}/{uuid}.normal.indel.bam".format(**globals()),
                       inputs={"{data}/{uuid}.normal.intervals".format(**globals())},
                       outputs={"{data}/{uuid}.normal.indel.bam".format(**globals())})

        s3_IR_t = Step(command="java -Xmx{HMEM}g -jar GenomeAnalysisTK.jar \
                                -T IndelRealigner \
                                -R {data}/genome.fa \
                                -I {data}/{TUMOUR}  \
                                -targetIntervals {data}/{uuid}.output.tumour.intervals \
                                --downsampling_type NONE \
                                -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                -maxReads 720000 -maxInMemory 5400000 \
                                -o {data}/{uuid}.tumour.indel.bam".format(**globals()),
                       inputs={"{data}/{uuid}.tumour.intervals".format(**globals())},
                       outputs={"{data}/{uuid}.tumour.indel.bam".format(**globals())})

        s4_BR_n = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T BaseRecalibrator \
                                -nct {cores} \
                                -R {data}/genome.fa \
                                -I {data}/{uuid}.normal.indel.bam \
                                -knownSites {data}/dbsnp_132_b37.leftAligned.vcf \
                                -o {data}/{uuid}.normal.recal_data.table".format(**globals()),
                       inputs={"{data}/{uuid}.normal.indel.bam".format(**globals())},
                       outputs={"{data}/{uuid}.normal.recal_data.table".format(**globals())})

        s4_BR_t = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T BaseRecalibrator \
                                -nct {cores} \
                                -R {data}/genome.fa \
                                -I {data}/{uuid}.tumour.indel.bam \
                                -knownSites {data}/dbsnp_132_b37.leftAligned.vcf \
                                -o {data}/{uuid}.tumour.recal_data.table".format(**globals()),
                       inputs={"{data}/{uuid}.tumour.indel.bam".format(**globals())},
                       outputs={"{data}/{uuid}.tumour.recal_data.table".format(**globals())})

        s5_PR_n = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T PrintReads \
                                -nct {cores}  \
                                -R {data}/genome.fa \
                                --emit_original_quals  \
                                -I {data}/{uuid}.normal.indel.bam \
                                -BQSR {data}/{uuid}.normal.recal_data.table \
                                -o {data}/{uuid}.normal.bqsr.bam".format(**globals()),
                       inputs={"{data}/{uuid}.normal.recal_data.table".format(**globals())},
                       outputs={"{data}/{uuid}.normal.bqsr.bam".format(**globals())})

        s5_PR_t = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T PrintReads \
                                -nct {cores} \
                                -R {data}/genome.fa \
                                --emit_original_quals  \
                                -I {data}/{uuid}.tumour.indel.bam \
                                -BQSR {data}/{uuid}.tumour.recal_data.table \
                                -o {data}/{uuid}.tumour.bqsr.bam".format(**globals()),
                       inputs={"{data}/{uuid}.tumour.recal_data.table".format(**globals())},
                       outputs={"{data}/{uuid}.tumour.bqsr.bam".format(**globals())})

        s6_CAF = Step(command="",
                      inputs={},
                      outputs={})