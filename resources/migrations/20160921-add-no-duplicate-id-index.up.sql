-- results_id increases monotonically, so sort desc; it will not be null, so we
-- must override the default to see any improvement
create index xml_tree_values_no_duplicate_id_idx
          on xml_tree_values (results_id desc nulls last, value, path)
where path ~ 'VipObject.0.*.id';
