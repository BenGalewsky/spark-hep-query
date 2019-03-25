def udf({% for col in cols %}{{col}}{{ "," if not loop.last }}{% endfor %}):
    import pandas as pd
    import numpy as np
    from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray
    from pydoc import locate

    my_class = locate('{{analysis_class}}')
    print(my_class)
    physics_objects = {}
    {% for obj in physics_objects.keys() %}
    physics_objects["{{obj}}"] = \
        JaggedCandidateArray.candidatesfromcounts(
            {% for zip_entry in physics_objects.get(obj) %}{{zip_entry}}{{ "," if not loop.last }}
            {% endfor %})
    {% endfor %}

    my_class.calc(physics_objects)
    return {{return_expr}}