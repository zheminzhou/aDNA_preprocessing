import math, gzip
def readFastq(filename):
    name, seq, isqual = '', {}, 0
    fastq = gzip.open(filename) if filename[-3:].lower() == '.gz' else open(filename)

    for line in fastq:
        if name != '' and isqual == 1 and len(seq[name][0]) > len(seq[name][1]) :
            seq[name][1].append(line.strip())
        elif line.startswith('@') :
            name = line[1:].strip().split()[0]
            seq[name] = [[], []]
            isqual = 0
        elif line.strip() == '+' :
            isqual = 1
        else :
            seq[name][isqual].append(line.strip())
    fastq.close()
    for name in seq:
        seq[name] = [''.join(seq[name][0]), ''.join(seq[name][1])]
    return seq

def readFasta(filename, qual=None) :
    seq = {}
    fin = gzip.open(filename) if filename[-3:].lower() == '.gz' else open(filename)
    
    for line in fin:
        if line[0] == '>' :
            name = line[1:].strip().split()[0]
            seq[name] = []
        else :
            seq[name].append( line.strip() )
    fin.close()
    if qual == None :
        for n in seq:
            seq[n] = ''.join( seq[n] )
    else :
        for n in seq:
            seq[n] = [''.join(seq[n]), []]
            seq[n][1] = [chr(33+qual)] * len(seq[n][0])
            if qual > 0 :
                for id in xrange(len(seq[n][0])) :
                    if seq[n][0] in ('n', 'N') :
                        seq[n][1] = '!'
            seq[n][1] = ''.join(seq[n][1])
    return seq
def readFasta_toList(filename, qual=None) :
    seq = []
    fin = gzip.open(filename) if filename[-3:].lower() == '.gz' else open(filename)
    
    for line in fin:
        if line[0] == '>' :
            name = line[1:].strip().split()[0]
            seq.append([name, []])
        else :
            seq[-1][1].append( line.strip() )
    fin.close()
    if qual == None :
        for id, (n, s) in enumerate(seq):
            seq[id][1] = ''.join( s )
    else :
        for id, (n, s) in enumerate(seq):
            seq[id][1] = ''.join(s)
            seq[id].append([chr(33+qual)] * len(seq[id][1]))
        if qual > 0 :
            for id in xrange(len(seq[id][1])) :
                if seq[id][1] in ('n', 'N') :
                    seq[id][2] = '!'
        seq[id][2] = ''.join(seq[id][2])
    return seq

def readXmfa(xmfa_file) :
    names, seq = [], {}
    fin = gzip.open(xmfa_file) if xmfa_file[-3:].lower() == '.gz' else open(xmfa_file)
    
    for line in fin :
        if line.startswith('>') :
            name = line[1:].strip().split()[0]
            if name not in seq :
                names.append( name )
                seq[name] = []
        elif line.startswith('=') :
            max_num, ref_name = max( [ [len(s), n] for n, s in seq.iteritems() ] )
            for n,s in seq.iteritems() :
                for block_id in xrange( len(s), max_num ) :
                    s.append('-' * len(seq[ref_name][block_id]) if seq[ref_name][block_id] != '=' else '=')
                s.append('=')
        else :
            seq[name].append(line.strip())
    fin.close()
    return [ [name, ''.join(seq[name])] for name in names ]

def parse_snps(seq, count_invariant=False) :
    if isinstance(seq, dict) :
        seq = sorted(seq.items())
    snps = [['#seq', 'site'] + [s[0] for s in seq]]
    invariant = {}
    block_status = [1, 0]
    for id, bases in enumerate(zip(*[s[1] for s in seq])) :
        if bases[0] == '=' :
            block_status = [ block_status[0]+1, 0 ]
        else :
            block_status[1] += 1
            types = set(bases) - set('-')
            if len(types) > 1 :
                snps.append(block_status+list(bases))
            elif count_invariant and len(types) > 0 :
                b = list(types)[0]
                invariant[ b ] = invariant.get(b, 0) + 1
    if count_invariant :
        return snps, sorted(invariant.items())
    else :
        return snps


def qualFilter(seq, cut = 10, output_file = None, modifyFastq = False) :
    new_seq = {}
    for name in seq:
        s = list(seq[name][0])
        q = seq[name][1]
        
        if cut > 0:
            cut2 = cut + 33
            for id in xrange(len(q)) :
                if ord(q[id]) >= cut2 :
                    s[id] = iupac_decode( s[id] )[0]
                else :
                    s[id] = 'N'
        new_seq[name] = ''.join(s)
        if modifyFastq == True :
            seq[name][0] = new_seq[name]
    if output_file != None:
        with open(output_file, 'w') as fout:
            for name, s in new_seq.iteritems():
                fout.write( '>{0}\n{1}\n'.format( name, s ) )
    return new_seq

def readNucmer(delta_file, open_penalty=2.0, extend_penalty=0.25, length_correction=0.0) :
    '''
    Return a list of alignments:
    ref_name, qry_name, ref_len, qry_len, ref_start, ref_end, qry_start, qry_end, num_diff, identities, score, aln_len, ...mappings...
    '''
    regions = []
    with open(delta_file) as fin:
        ref_name, gap_open, gap_extend, prev = '', 0, 0, 0
    for line in fin :
        if line[0] == '>' :
            ref_name, qry_name, ref_len, qry_len = line[1:].strip().split(' ')
        elif ref_name != '' :
            part = line.strip().split(' ')
        if len(part) > 1 :
            regions.append([ref_name, qry_name, ref_len, qry_len] + part)
        elif part[0] != '0' :
            tag = int(part[0])
            if (tag == 1 and prev > 0) or (tag == -1 and prev < 0) :
                gap_extend += 1
            else :
                gap_open += 1
            regions[-1].append(tag)
            prev = tag
        else :
            region = regions[-1]
            region[2:11] = [int(x) for x in region[2:11]]
            region.append( (region[5] - region[4]+1)  + len([site for site in region[11:] if site < 0]) )
            region[9] = 100.0 - 100 * (region[8] - gap_extend - gap_open + open_penalty*gap_open + extend_penalty*gap_extend)/region[-1]
            region[10] = region[9] * math.pow(region[-1], length_correction)
            prev, gap_extend, gap_open = 0, 0, 0
    return regions

def iupac_encode(base1, base2) :
    if base2 == '' or base2[0] == '<' : return base1.upper()
    table = {'AG': 'R', 'CT': 'Y', 'CG': 'S', 'AT': 'W', 'GT': 'K', 'AC': 'M', 
             'GA': 'r', 'TC': 'y', 'GC': 's', 'TA': 'w', 'TG': 'k', 'CA': 'm'}
    return table.get(base1+base2, 'N')

def iupac_decode(code) :
    table = {'R': ['A','G'], 'Y': ['C','T'], 'S': ['C','G'], 'W': ['A','T'], 'K': ['G','T'], 'M': ['A','C'], 
             'r': ['G','A'], 'y': ['T','C'], 's': ['G','C'], 'w': ['T','A'], 'k': ['T','G'], 'm': ['C','A'],
             'A': ['A',''], 'C': ['C',''], 'G': ['G',''], 'T': ['T',''], 
             'a': ['A',''], 'c': ['C',''], 'g': ['G',''], 't': ['T','']}
    return table.get(code, ['N', ''])

codons = {"TTT":"F", "TTC":"F", "TTA":"L", "TTG":"L",
          "TCT":"S", "TCC":"S", "TCA":"S", "TCG":"S",
          "TAT":"Y", "TAC":"Y", "TAA":"X", "TAG":"X",
          "TGT":"C", "TGC":"C", "TGA":"X", "TGG":"W",
          "CTT":"L", "CTC":"L", "CTA":"L", "CTG":"L",
          "CCT":"P", "CCC":"P", "CCA":"P", "CCG":"P",
          "CAT":"H", "CAC":"H", "CAA":"Q", "CAG":"Q",
          "CGT":"R", "CGC":"R", "CGA":"R", "CGG":"R",
          "ATT":"I", "ATC":"I", "ATA":"I", "ATG":"M",
          "ACT":"T", "ACC":"T", "ACA":"T", "ACG":"T",
          "AAT":"N", "AAC":"N", "AAA":"K", "AAG":"K",
          "AGT":"S", "AGC":"S", "AGA":"R", "AGG":"R",
          "GTT":"V", "GTC":"V", "GTA":"V", "GTG":"V",
          "GCT":"A", "GCC":"A", "GCA":"A", "GCG":"A",
          "GAT":"D", "GAC":"D", "GAA":"E", "GAG":"E",
          "GGT":"G", "GGC":"G", "GGA":"G", "GGG":"G"}

complement = {'A':'T', 'T':'A', 'G':'C', 'C':'G'}

codons4 = dict(codons.items())
codons4['TGA'] = 'W'

def rc(seq) :
    return ''.join([complement.get(b, 'N') for b in reversed(seq.upper())])

def transeq(seq, frame=1, transl_table=11) :
    gtable = codons if transl_table != 4 else codons4
    if str(frame).upper() == 'F' :
        frames = [1,2,3]
    elif str(frame).upper() == 'R' :
        frames = [4,5,6]
    elif str(frame).upper() == '7' :
        frames = range(1,7)
    else :
        frames = [int(frame)]

    if isinstance(seq, dict) :
        trans_seq = {}
        for n,s in seq.iteritems() :
            for frame in frames :
                trans_name = '{0}_{1}'.format(n, frame)
            if frame <= 3 :
                trans_seq[trans_name] = ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(s[(frame-1):])]*3))])
            else :
                trans_seq[trans_name] = ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(rc(s)[(frame-4):])]*3))])
        return trans_seq
    else :
        if len(frames) == 1 :
            frame = frames[0]
            if frame <= 3 :
                return ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(seq[(frame-1):])]*3))])
            else :
                return ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(rc(seq)[(frame-4):])]*3))])
        else :
            trans_seq = []
            for frame in frames :
                if frame <= 3 :
                    trans_seq.append( ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(seq[(frame-1):])]*3))]) )
                else :
                    trans_seq.append( ''.join([gtable.get(c, 'X') for c in map(''.join, zip(*[iter(rc(seq)[(frame-4):])]*3))]) )
            return trans_seq
