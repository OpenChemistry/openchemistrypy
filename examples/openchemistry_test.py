import openchemistry as oc

calc_setup = {
    'basis': '3-21g', # What fields in db?
    'theory': 'dft'
}

mymol = oc.find_structure('RYYVLZVUVIJVGH-UHFFFAOYSA-N')
result = mymol.optimize(**calc_setup)
result.structure.show()

result = mymol.frequencies(**calc_setup)
result.frequencies.show()
freq_table = result.frequencies.table()
print(freq_table)
