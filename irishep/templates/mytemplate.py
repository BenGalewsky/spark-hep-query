def udf({% for col in cols %}{{col}}{{ "," if not loop.last }}{% endfor %}):
    import pandas as pd
    import numpy as np
    from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray
    global my_analysis

    physics_objects = {}
    {% for obj in physics_objects.keys() %}
    physics_objects["{{obj}}"] = \
        JaggedCandidateArray.candidatesfromcounts(
            {{counts[obj]}}.values, {% for column in physics_objects[obj] %}
            {{column['physics_obj_property']}}={{column['col']}}.array[0].base {{ "," if not loop.last }}{% endfor %})
    {% endfor %}

    return pd.Series(my_analysis.calc(physics_objects, dataset[0]))
