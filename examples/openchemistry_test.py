import openchemistry as oc

calc_setup = {
    'basis': '3-21g',
    'theory': 'dft'
}

mymol = oc.find_structure('RYYVLZVUVIJVGH-UHFFFAOYSA-N')
result = mymol.optimize(**calc_setup)
result.structure.show()
