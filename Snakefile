#!/usr/bin/env python3

import multiprocessing
import os
import snakemake

###########
# GLOBALS #
###########

racon_container = 'shub://TomHarrop/singularity-containers:racon_1.3.2'

chunk_dir = 'data/chunk_sam'
fasta = 'data/flye_denovo_full.racon.fasta'

########
# MAIN #
########

all_chunks = snakemake.io.glob_wildcards(
    os.path.join(chunk_dir, 'aln.sam.{chunk_no}.bam')).chunk_no

#########
# RULES #
#########

rule target:
    input:
        expand('output/chunk_{chunk_no}/racon.fasta',
               chunk_no=all_chunks)

rule racon:
    input:
        fasta = fasta,
        aln = os.path.join(chunk_dir, 'aln.sam.{chunk_no}.bam'),
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
