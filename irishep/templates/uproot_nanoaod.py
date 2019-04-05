def udf(arrays):
    import pandas as pd
    import numpy as np
    from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray
    global my_analysis

    physics_objects = {}
    {% for obj in physics_objects.keys() %}
    physics_objects["{{obj}}"] = \
        JaggedCandidateArray.candidatesfromcounts(arrays['{{counts[obj]}}'], {% for column in physics_objects[obj] %}
            {{column['physics_obj_property']}}=arrays["{{column['col']}}"].content {{ "," if not loop.last }}{% endfor %})
    {% endfor %}

    return my_analysis.calc(physics_objects, arrays["dataset"][0])
