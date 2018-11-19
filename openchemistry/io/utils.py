def _cclib_to_cjson_basis(basis):
    shell_type_map = {
        's': 0,
        'p': 1,
        'd': 2,
        'f': 3,
        'g': 4,
        'h': 5,
        'i': 6,
        'k': 7,
        'l': 8,
        'm': 9,
        'n': 10,
        'o': 11
    }
    coefficients = []
    exponents = []
    primitives_per_shell = []
    shell_to_atom_map = []
    shell_types = []
    for i_atom, atom_basis in enumerate(basis):
        for shell in atom_basis:
            l_label, primitives = shell
            n_primitives = len(primitives)
            shell_to_atom_map.append(i_atom)
            primitives_per_shell.append(n_primitives)
            shell_types.append(shell_type_map[l_label.lower()])
            for primitive in primitives:
                exponents.append(primitive[0])
                coefficients.append(primitive[1])
    cjson_basis = {
        'coefficients': coefficients,
        'exponents': exponents,
        'primitivesPerShell': primitives_per_shell,
        'shellToAtomMap': shell_to_atom_map,
        'shellTypes': shell_types
    }
    return cjson_basis

def _cclib_to_cjson_mocoeffs(coeffs):
    cjson_coeffs = []
    # only take the orbitals at the end of the optimization
    for mo in coeffs[-1]:
        cjson_coeffs.extend(mo)
    return cjson_coeffs

def _cclib_to_cjson_vibdisps(vibdisps):
    cjson_vibdisps = []
    for vibdisp in vibdisps:
        cjson_vibdisps.append(list(vibdisp.flatten()))
    return cjson_vibdisps
