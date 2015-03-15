__author__ = 'CJ'
import os
import subprocess
import shutil

from workflow import WorkFlow
from workflow import Step
from scheduler import Connection

# Before testing: set up queue, change connection object to reflect new queue/bucket. Also add general logging.

# FIXME: Automatically get/set UUID from files so these commands work properly. For now they are gonna use fake uuids.

# FIXME: get rid of format() calls after command field only. these will be formatted in main. Not essential, just style

# FIXME: Associate function calls with steps so we dont have to call delete_files every other step.

# FIXME: seperate workflow class and its usage (move the stuff in main to another file). Right now we cant, since we
# FIXME:    need access to the globals

# FIXME: add parrallel step class. Should accept two other steps in constructor. run/join them as threads

# globals for formatting

data = '/home/ubuntu/data'
tool_dir = '/home/ubuntu/tools'

memory = 15
half_memory = memory/2
cores = 4

uuid='123456789'
normal=uuid + '.N.bam'
tumor=uuid + '.T.bam'

normalBai = normal + '.bai'
tumorBai = tumor + '.bai'

contam = None

current_bucket = None
normal_key = None
tumor_key = None

# Specify protected input set
input_set = {'{data}/1000G_phase1.indels.hg19.sites.fixed.vcf'.format(**globals()),
             '{data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf'.format(**globals()),
             '{data}/dbsnp_132_b37.leftAligned.vcf'.format(**globals()),
             '{data}/SNP6.hg19.interval_list'.format(**globals()),
             '{data}/gaf_20111020+broad_wex_1.1_hg19.bed'.format(**globals()),
             '{data}/hg19_population_stratified_af_hapmap_3.3.fixed.vcf'.format(**globals()),
             '{data}/b37_cosmic_v54_120711.vcf',
             '{data}/genome.fa'.format(**globals()),
             '{data}/genome.dict'.format(**globals()),
             '{data}/genome.fa.fai'.format(**globals())}


class PCAWG(object, WorkFlow):

    def __init__(self):
        super(PCAWG, self).__init__()
        # in these funtion steps, the function parameter is set to the function itself, not its results- that is why
        # the function fields are function=self.get_contam, NOT self.get_contam()

        self.delete_step = Step(command="", function=self.delete_files, inputs=set(), outputs=set())

        self.get_contam = Step(command="", function=self.calculate_contamination,
                               inputs={'{data}/contest.firehose'.format(**globals())},
                               outputs=set())

        self.steps = [self.downloadTumor, self.downloadNormal, self.s1_index_p, self.delete_step, self.s2_RTC_n,
                      self.delete_step, self.s2_RTC_t, self.delete_step, self.s3_IR_n,
                      self.delete_step, self.s3_IR_t, self.delete_step, self.s4_BR_n,
                      self.delete_step, self.s4_BR_t, self.delete_step, self.s5_PR_n,
                      self.delete_step, self.s5_PR_t, self.delete_step, self.s6_CAF, self.get_contam, self.s7_Mu ]
    """
    naming: step#, description, p= parallel, n = normal, t = tumor steps
    """
    downloadNormal = Step(command="aws s3 cp s3://{current_bucket}/{normal_key} {data}/{normal}".format(**globals()),
                           inputs={None},
                           outputs={"{normal}".format(**globals())})

    downloadTumor = Step(command="aws s3 cp s3://{current_bucket}/{tumor_key} {data}/{tumor}".format(**globals()),
                           inputs={None},
                           outputs={"{tumor}".format(**globals())})

    s1_index_p = Step(command="samtools index {data}/{normal} & samtools index {data}/{tumor} & wait".format(**globals()),
                      inputs={normal, tumor},
                      outputs={normalBai, tumorBai})

    s2_RTC_n = Step(command="java -Xmx{memory}g -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T RealignerTargetCreator \
                            -nt {cores} \
                            -R {data}/genome.fa \
                            -I {data}/{normal} \
                            -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                            -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                            --downsampling_type NONE \
                            -o {data}/{uuid}.normal.intervals".format(**globals()),
                    inputs={normal, normalBai} | input_set,
                    outputs={"{data}/{uuid}.normal.intervals".format(**globals())})

    s2_RTC_t = Step(command="java -Xmx{memory}g -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T RealignerTargetCreator \
                            -nt {cores} \
                            -R {data}/genome.fa \
                            -I {data}/{tumor} \
                            -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                            -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                            --downsampling_type NONE \
                            -o {data}/{uuid}.tumour.intervals".format(**globals()),
                    inputs={tumor, tumorBai} | input_set,
                    outputs={"{data}/{uuid}.tumour.intervals".format(**globals())})

    s3_IR_n = Step(command="java -Xmx{half_memory}g -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T IndelRealigner \
                            -R {data}/genome.fa \
                            -I {data}/{normal}  \
                            -targetIntervals {data}/{uuid}.normal.intervals \
                            --downsampling_type NONE \
                            -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                            -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                            -maxReads 720000 -maxInMemory 5400000 \
                            -o {data}/{uuid}.normal.indel.bam".format(**globals()),
                   inputs={"{data}/{uuid}.normal.intervals".format(**globals())} | input_set,
                   outputs={"{data}/{uuid}.normal.indel.bam".format(**globals()),
                            "{data}/{uuid}.normal.indel.bai".format(**globals())})

    s3_IR_t = Step(command="java -Xmx{half_memory}g -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T IndelRealigner \
                            -R {data}/genome.fa \
                            -I {data}/{tumor}  \
                            -targetIntervals {data}/{uuid}.output.tumour.intervals \
                            --downsampling_type NONE \
                            -known {data}/1000G_phase1.indels.hg19.sites.fixed.vcf \
                            -known {data}/Mills_and_1000G_gold_standard.indels.hg19.sites.fixed.vcf \
                            -maxReads 720000 -maxInMemory 5400000 \
                            -o {data}/{uuid}.tumour.indel.bam".format(**globals()),
                   inputs={"{data}/{uuid}.tumour.intervals".format(**globals())} | input_set,
                   outputs={"{data}/{uuid}.tumour.indel.bam".format(**globals()),
                            "{data}/{uuid}.tumour.indel.bai".format(**globals())})

    s4_BR_n = Step(command="java -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T BaseRecalibrator \
                            -nct {cores} \
                            -R {data}/genome.fa \
                            -I {data}/{uuid}.normal.indel.bam \
                            -knownSites {data}/dbsnp_132_b37.leftAligned.vcf \
                            -o {data}/{uuid}.normal.recal_data.table".format(**globals()),
                   inputs={"{data}/{uuid}.normal.indel.bam".format(**globals())} | input_set,
                   outputs={"{data}/{uuid}.normal.recal_data.table".format(**globals())})

    s4_BR_t = Step(command="java -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T BaseRecalibrator \
                            -nct {cores} \
                            -R {data}/genome.fa \
                            -I {data}/{uuid}.tumour.indel.bam \
                            -knownSites {data}/dbsnp_132_b37.leftAligned.vcf \
                            -o {data}/{uuid}.tumour.recal_data.table".format(**globals()),
                   inputs={"{data}/{uuid}.tumour.indel.bam".format(**globals())},
                   outputs={"{data}/{uuid}.tumour.recal_data.table".format(**globals())})

    s5_PR_n = Step(command="java -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T PrintReads \
                            -nct {cores}  \
                            -R {data}/genome.fa \
                            --emit_original_quals  \
                            -I {data}/{uuid}.normal.indel.bam \
                            -BQSR {data}/{uuid}.normal.recal_data.table \
                            -o {data}/{uuid}.normal.bqsr.bam".format(**globals()),
                   inputs={"{data}/{uuid}.normal.recal_data.table".format(**globals())} | input_set,
                   outputs={"{data}/{uuid}.normal.bqsr.bam".format(**globals()),
                            "{data}/{uuid}.normal.bqsr.bai".format(**globals())})

    s5_PR_t = Step(command="java -jar {tool_dir}/GenomeAnalysisTK.jar \
                            -T PrintReads \
                            -nct {cores} \
                            -R {data}/genome.fa \
                            --emit_original_quals  \
                            -I {data}/{uuid}.tumour.indel.bam \
                            -BQSR {data}/{uuid}.tumour.recal_data.table \
                            -o {data}/{uuid}.tumour.bqsr.bam".format(**globals()),
                   inputs={"{data}/{uuid}.tumour.recal_data.table".format(**globals())} | input_set,
                   outputs={"{data}/{uuid}.tumour.bqsr.bam".format(**globals()),
                            "{data}/{uuid}.tumour.bqsr.bai".format(**globals())})

    s6_CAF = Step(command="java -Djava.io.tmpdir=~/tmp -Xmx2g \
                            -jar {tool_dir}/Queue-1.4-437-g6b8a9e1-svn-35362.jar \
                            -S {tool_dir}/ContaminationPipeline.scala \
                            --reference {data}/genome.fa \
                            --output {data}/contest \
                            --bamfile {data}/{uuid}.tumour.bqsr.bam \
                            -nbam {data}/{uuid}.normal.bqsr.bam \
                            --popfile {data}/hg19_population_stratified_af_hapmap_3.3.fixed.vcf \
                            --arrayinterval {data}/SNP6.hg19.interval_list \
                            --interval {data}/gaf_20111020+broad_wex_1.1_hg19.bed \
                            -run -memory 2".format(**globals()),
                  inputs={"{uuid}.tumour.bqsr.bam".format(**globals()),
                          "{uuid}.normal.bqsr.bam".format(**globals())} | input_set,
                  outputs={"{data}/contest".format(**globals()),
                           "{data}/contest.base_report.txt".format(**globals()),
                           "{data}/contest.firehose".format(**globals()),
                           "{data}/contest.firehouse.out".format(**globals()),
                           "{data}/contest.out".format(**globals()),
                           "{data}/.contest.done".format(**globals()),
                           "{data}/.contest.firehose.done".format(**globals()),
                           "{data}/.contest.firehose.out.done".format(**globals()),
                           "{data}/.contest.out.done".format(**globals())})
    # The calculate_contamination function cannot be called until after S6_CAF
    # How do we handle .format() with globals and a non-global value? Not sure if the
    # below implementation will work where contam is being substituted to {0}.
    # The alternative is to create the class object, call the step methods, and
    # in between S6 and S7 make a call to the calculate_contamination() function.

    # TEMPORARY FIX: format is not called on this command. We will format all commands before check_calling them
    # this doesn't do anything to commands already formated, and will allow this command to be formatted after
    # get_contamination step is called. This can be cleaned up by removing the format call after each COMMAND ONLY,
    # leave the input/output as is.
    s7_Mu = Step(command="java -Xmx4g -jar muTect-1.1.5.jar \
                            --analysis_type MuTect \
                            --reference_sequence {data}/genome.fa \
                            --cosmic  {data}/b37_cosmic_v54_120711.vcf \
                            --dbsnp {data}/dbsnp_132_b37.leftAligned.vcf \
                            --intervals {data}/SNP6.hg19.interval_list \
                            --input_file:normal {data}/normal.bqsr.bam \
                            --input_file:tumor {data}/tumour.bqsr.bam \
                            --fraction_contamination {0} \
                            --out {data}/MuTect.out \
                            --coverage_file {data}/MuTect.coverage \
                            --vcf {data}/MuTect.pair8.vcf"
                 ,
                 inputs={"{uuid}.tumour.bqsr.bam".format(**globals()),
                         "{uuid}.normal.bqsr.bam".format(**globals())} | input_set,
                 outputs={"{data}/MuTect.coverage".format(**globals()),
                          "{data}/MuTect.out".format(**globals()),
                          "{tool_dir}/ContaminationPipeline.jobreport.txt".format(**globals())})

    def calculate_contamination(self):
        """
        Open up contamination file and return contamination value
        """
        global contam
        contam = 0.01
        with open('{data}/contest.firehose'.format(**globals()), 'r') as file:
            val = file.readline()
            contam *= float(val)


def main():
    connection = Connection(region="us-west-2",
                            bucket_one='bd2k-test-flow-start',
                            bucket_two='bd2k-test-flow-intermediate',
                            bucket_three='bd2k-test-flow-final',
                            queue_one='bd2k-queue-start',
                            queue_two='bd2k-queue-intermediate')
    global current_bucket
    current_bucket = connection.get_bucket_name()
    global normal_key
    global tumor_key
    normal_key,tumor_key = connection.get_keys()

    workflow = PCAWG()

    for step in workflow.steps:
        subprocess.check_call(step.command.format(**globals()), shell=True)
        step.function()


if __name__ == '__main__':
    main()