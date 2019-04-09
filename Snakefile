#!/usr/bin/env python3

import multiprocessing
import os
import snakemake

###########
# GLOBALS #
###########

racon_container = 'shub://TomHarrop/singularity-containers:racon_1.3.2'
samtools_container = 'shub://TomHarrop/singularity-containers:samtools_1.9'

chunk_dir = 'data/chunk_sam'
fasta = 'data/flye_denovo_full.racon.fasta'

########
# MAIN #
########

# chunks that have hung on me
bad_chunks = ['1096', '1191']

all_chunks = snakemake.io.glob_wildcards(
    os.path.join(chunk_dir, 'aln.sam.{chunk_no}.bam')).chunk_no

chunks_to_try = [x for x in all_chunks if x not in bad_chunks]

#########
# RULES #
#########

rule target:
    input:
        expand('output/chunk_{chunk_no}/racon.fasta',
               chunk_no=chunks_to_try)

rule racon:
    input:
        fasta = fasta,
        aln = 'output/tmp/{chunk_no}.sam',
        fq = os.path.join(chunk_dir, 'aln.sam.{chunk_no}.fastq')
    output:
        'output/chunk_{chunk_no}/racon.fasta'
    log:
        'output/logs/chunk_{chunk_no}.log'
    threads:
        multiprocessing.cpu_count()
    singularity:
        racon_container
    shell:
        'racon '
        '-t {threads} '
        '{input.fq} '
        '{input.aln} '
        '{input.fasta} '
        '> {output} '
        '2> {log}'

rule convert:
    input:
        bam = os.path.join(chunk_dir, 'aln.sam.{chunk_no}.bam')
    output:
        sam = 'output/tmp/{chunk_no}.sam'
    singularity:
        samtools_container
    shell:
        'samtools view -h -o {output.sam} {input.bam}'
