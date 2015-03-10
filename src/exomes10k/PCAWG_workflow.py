__author__ = 'CJ'
import os
import unittest
import subprocess
import tempfile
import shutil

from exomes10k.workflow import WorkFlow
from exomes10k.workflow import Step

data="/home/ubuntu/data"
memory=15
halfMemory= memory/2

core=4

uuid='123456789'
normal=uuid + '.N.bam'
tumor=uuid + '.T.bam'

normalBai = normal + '.bai'
tumorBai = tumor + '.bai'

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
                                -nt {CORES} \
                                -R {DATA}/genome.fa \
                                -I {DATA}/{NORMAL} \
                                -known {DATA}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {DATA}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                --downsampling_type NONE \
                                -o {DATA}/{UUID}.normal.intervals".format(**globals()),
                        inputs={normal, normalBai},
                        outputs={"{DATA}/{UUID}.normal.intervals".format(**globals())})

        s2_RTC_t = Step(inputs="java -Xmx{MEM}g -jar GenomeAnalysisTK.jar \
                                -T RealignerTargetCreator \
                                -nt {CORES} \
                                -R ${DATA}/genome.fa \
                                -I ${DATA}/${TUMOUR} \
                                -known ${DATA}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known ${DATA}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                --downsampling_type NONE \
                                -o {DATA}/{UUID}.tumour.intervals".format(**globals()),
                        inputs={tumor, tumorBai},
                        outputs={"{DATA}/{UUID}.tumour.intervals".format(**globals())})

        s3_IR_n = Step(command="java -Xmx{HMEM}g -jar GenomeAnalysisTK.jar \
                                -T IndelRealigner \
                                -R {DATA}/genome.fa \
                                -I {DATA}/{NORMAL}  \
                                -targetIntervals {DATA}/{UUID}.normal.intervals \
                                --downsampling_type NONE \
                                -known {DATA}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {DATA}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                -maxReads 720000 -maxInMemory 5400000 \
                                -o {DATA}/{UUID}.normal.indel.bam".format(**globals()),
                       inputs={"{DATA}/{UUID}.normal.intervals".format(**globals())},
                       outputs={"{DATA}/{UUID}.normal.indel.bam".format(**globals())})

        s3_IR_t = Step(command="java -Xmx{HMEM}g -jar GenomeAnalysisTK.jar \
                                -T IndelRealigner \
                                -R {DATA}/genome.fa \
                                -I {DATA}/{TUMOUR}  \
                                -targetIntervals {DATA}/{UUID}.output.tumour.intervals \
                                --downsampling_type NONE \
                                -known {DATA}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                                -known {DATA}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                                -maxReads 720000 -maxInMemory 5400000 \
                                -o {DATA}/{UUID}.tumour.indel.bam".format(**globals()),
                       inputs={"{DATA}/{UUID}.tumour.intervals".format(**globals())},
                       outputs={"{DATA}/{UUID}.tumour.indel.bam".format(**globals())})

        s4_BR_n = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T BaseRecalibrator \
                                -nct {CORES} \
                                -R {DATA}/genome.fa \
                                -I {DATA}/{UUID}.normal.indel.bam \
                                -knownSites {DATA}/dbsnp_132_b37.leftAligned.vcf \
                                -o {DATA}/{UUID}.normal.recal_data.table".format(**globals()),
                       inputs={"{DATA}/{UUID}.normal.indel.bam".format(**globals())},
                       outputs={"{DATA}/{UUID}.normal.recal_data.table".format(**globals())})

        s4_BR_t = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T BaseRecalibrator \
                                -nct {CORES} \
                                -R {DATA}/genome.fa \
                                -I {DATA}/{UUID}.tumour.indel.bam \
                                -knownSites {DATA}/dbsnp_132_b37.leftAligned.vcf \
                                -o {DATA}/{UUID}.tumour.recal_data.table".format(**globals()),
                       inputs={"{DATA}/{UUID}.tumour.indel.bam".format(**globals())},
                       outputs={"{DATA}/{UUID}.tumour.recal_data.table".format(**globals())})

        s5_PR_n = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T PrintReads \
                                -nct {CORES}  \
                                -R {DATA}/genome.fa \
                                --emit_original_quals  \
                                -I {DATA}/{UUID}.normal.indel.bam \
                                -BQSR {DATA}/{UUID}.normal.recal_data.table \
                                -o {DATA}/{UUID}.normal.bqsr.bam".format(**globals()),
                       inputs={"{DATA}/{UUID}.normal.recal_data.table".format(**globals())},
                       outputs={"{DATA}/{UUID}.normal.bqsr.bam".format(**globals())})

        s5_PR_t = Step(command="java -jar GenomeAnalysisTK.jar \
                                -T PrintReads \
                                -nct {CORES} \
                                -R {DATA}/genome.fa \
                                --emit_original_quals  \
                                -I {DATA}/{UUID}.tumour.indel.bam \
                                -BQSR {DATA}/{UUID}.tumour.recal_data.table \
                                -o {DATA}/{UUID}.tumour.bqsr.bam".format(**globals()),
                       inputs={"{DATA}/{UUID}.tumour.recal_data.table".format(**globals())},
                       outputs={"{DATA}/{UUID}.tumour.bqsr.bam".format(**globals())})