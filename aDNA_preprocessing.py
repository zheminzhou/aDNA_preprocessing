#! preprocessing

import sys, subprocess, os, gzip
from pyLib import configure_reader, logger

config = configure_reader(os.path.realpath(__file__)[:-3] + '.ini')['setting']
collapse_cmd, trim_cmd, trim_pe = config['collapse_cmd'], config['trim_cmd'], config['trim_pe']
human_db, bowtie2 = config['human_db'], config['bowtie2']
samtools, mapDamage = config['samtools'], config['mapDamage']
read_filter = config['read_filter']
params = [config['bbmap_dir'], config['adapter_db'], config['phiX_db']]

def bowtie_map(ref, se, pe, executable, reporting=1, mode='strict', n_thread=4, additional_params = '--no-unal --no-discordant --no-mixed --ignore-quals') :
    qry_format = '-U {0}'.format(','.join(se)) if len(se) > 0 else ''
    qry_format += ' -1 {0} -2 {1}'.format(','.join(pe[::2]), ','.join(pe[1::2])) if len(pe) > 1 else ''
    
    if mode =='strict' :
        map_mode = '--sensitive-local --mp 8,8 --np 8 --score-min L,15,1.35' 
    elif mode == 'loose' :
        map_mode = '--sensitive-local --mp 8,8 --np 8 --score-min L,18,1.1'
    else :
        map_mode = '--sensitive --end-to-end'

    cmd_bt2 = '{0} -t -q -p {1} -I 50 -X 800 -x {2} {3} {5} {6} {4}'.format( \
        executable, n_thread, ref, \
        '-k {0}'.format(reporting) if reporting > 1 else '', \
        qry_format, \
        map_mode, additional_params
    )
    bt2_run = subprocess.Popen(cmd_bt2.split(), stdout=subprocess.PIPE)
    for line in iter(bt2_run.stdout.readline, r'') :
        yield line
        

def dnaDamage(ref, qry, bowtie2, samtools, mapDamage, prefix='dnaDamage') :
    samtool_cmd = '{0} view -uo {2}.bam -@3 -T{1} -'.format(samtools, ref, prefix)
    samtool_run = subprocess.Popen(samtool_cmd.split(), stdin=subprocess.PIPE)
    for line in bowtie_map(ref, [qry], [], bowtie2, mode='global', n_thread=3, additional_params='--no-unal') :
        samtool_run.stdin.write(line)
    samtool_run.communicate()
    
    mapDamage_cmd = '{0} -i {2}.bam -d results_{2} -t {2} -r {1} --merge-reference-sequences'.format(mapDamage.format(PYTHON=sys.executable), ref, prefix)
    outfolder = 'results_' + prefix
    subprocess.Popen(mapDamage_cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE).communicate()
    os.rename(outfolder + '/Stats_out_MCMC_iter_summ_stat.csv', prefix + '.MD.damage_param.csv')
    os.rename(outfolder + '/misincorporation.txt', prefix + '.MD.damage_pattern.txt')
    os.rename(outfolder + '/dnacomp.txt', prefix + '.MD.dna_composition.txt')
    os.rename(outfolder + '/lgdistribution.txt', prefix + '.MD.lgdistribution.txt')

    os.rename(outfolder + '/Fragmisincorporation_plot.pdf', prefix + '.MD.Fragmisincorporation_plot.pdf')
    os.rename(outfolder + '/Length_plot.pdf', prefix + '.MD.Length_plot.pdf')
    os.rename(outfolder + '/Stats_out_MCMC_post_pred.pdf', prefix + '.MD.Damage_estimates.pdf')
    
    with open(prefix + '.MD.damage_param.csv') as fin :
        for line in fin :
            if line.startswith('"50%"') :
                deamination = float(line.strip().split(',')[3])
    
    import shutil
    shutil.rmtree(outfolder)
    
    return deamination, [prefix + '.damage_param.csv', prefix + '.damage_pattern.txt', prefix + '.dna_composition.txt', prefix + '.lgdistribution.txt', \
            prefix + '.Fragmisincorporation_plot.pdf', prefix + '.Length_plot.pdf', prefix + '.Damage_estimates.pdf']

if __name__ == '__main__' :
    task, sample, files = sys.argv[1].split(','), sys.argv[2], [f for f in sys.argv[3:] if len(f) > 0]
    if len(files) > 1 :
        a = subprocess.Popen(collapse_cmd.format(sample, files[0], files[1], *params).split(), stdout=subprocess.PIPE)
        a.communicate()
        if a.returncode != 0 :
            subprocess.Popen(collapse_cmd.format(sample, files[0], files[1], *params).split() + ['qin=33'], stdout=subprocess.PIPE).communicate()
        a = subprocess.Popen(trim_pe.format(sample + '.u1.fastq.gz', sample + '.u2.fastq.gz', sample, *params).split(), stdout=subprocess.PIPE)
        a.communicate()
        if a.returncode != 0 :
            subprocess.Popen(trim_pe.format(sample + '.u1.fastq.gz', sample + '.u2.fastq.gz', sample, *params).split() + ['qin=33'], stdout=subprocess.PIPE).communicate()
        a = subprocess.Popen(trim_cmd.format(sample + '.s2.fastq.gz', sample + '.s3.fastq.gz', *params).split(), stdout=subprocess.PIPE)
        a.communicate()
        if a.returncode != 0 :
            subprocess.Popen(trim_cmd.format(sample + '.s2.fastq.gz', sample + '.s3.fastq.gz', *params).split() + ['qin=33'], stdout=subprocess.PIPE).communicate()
        for f in (sample + '.u1.fastq.gz', sample + '.u2.fastq.gz', sample + '.s2.fastq.gz') :
            os.unlink(f)
        files = [sample + '.s1.fastq.gz', sample + '.s3.fastq.gz', sample + '.rd1.fastq.gz', sample + '.rd2.fastq.gz']
    else :
        a = subprocess.Popen(trim_cmd.format(files[0], sample + '.s1.fastq.gz', *params).split(), stdout=subprocess.PIPE)
        a.communicate()
        if a.returncode != 0 :
            subprocess.Popen(trim_cmd.format(files[0], sample + '.s1.fastq.gz', *params).split() + ['qin=33'], stdout=subprocess.PIPE).communicate()
        files = [sample + '.s1.fastq.gz']
    
    human_reads, outputs = {}, {}
    if 'deHuman' in task :
        with gzip.open(sample + '.human.fastq.gz', 'w') as fout :
            for line in bowtie_map(human_db, files[:2], files[2:], bowtie2, mode='loose', n_thread=3, additional_params='--no-unal --no-head') :
                part = line.strip().split()
                if abs(int(part[8])) + 10 < len(part[9]) :
                    fout.write('@{0}\n{1}\n+\n{2}\n'.format(part[0], part[9], part[10]))
                human_reads[part[0]] = 1
        outputs['human'] = {'num':len(human_reads)}
        if 'mapDamage' in task :
            outputs['human']['deamination'], outputs['human']['mapDamage_Files'] = dnaDamage(human_db, sample+'.human.fastq.gz', bowtie2, samtools, mapDamage, prefix=sample)

    with open(sample + '.human_reads', 'w') as fout :
        for r in human_reads :
            fout.write(r + '\n')
    rstat = subprocess.Popen(read_filter.format(' '.join(files[:2]), sample + '.human_reads', sample + '.se.fastq.gz', *params), shell=True, stderr=subprocess.PIPE).communicate()[1]
    outputs['SE'] = {'Files': sample + '.se.fastq.gz'}
    for line in rstat.split('\n') :
        if line.startswith('Reads Out') :
            outputs['SE']['Read No.'] = int(line.strip().split()[-1])
        elif line.startswith('Bases Out') :
            outputs['SE']['Base No.'] = int(line.strip().split()[-1])
    
    if len(files) == 4 :
        outputs['PE'] = {'Files': [sample + '.r1.fastq.gz', sample + '.r2.fastq.gz']}
        rstat = subprocess.Popen(read_filter.format(files[2], sample + '.human_reads', sample + '.r1.fastq.gz', *params), shell=True, stderr=subprocess.PIPE).communicate()[1]
        for line in rstat.split('\n') :
            if line.startswith('Reads Out') :
                outputs['PE']['Read No.'] = int(line.strip().split()[-1])
            elif line.startswith('Bases Out') :
                outputs['PE']['Base No.'] = int(line.strip().split()[-1])
        
        rstat = subprocess.Popen(read_filter.format(files[3], sample + '.human_reads', sample + '.r2.fastq.gz', *params), shell=True, stderr=subprocess.PIPE).communicate()[1]
        for line in rstat.split('\n') :
            if line.startswith('Bases Out') :
                outputs['PE']['Base No.'] += int(line.strip().split()[-1])
    for f in files :
        os.unlink(f)
    
    import json
    print json.dumps(outputs, indent=2, sort_keys=True)
