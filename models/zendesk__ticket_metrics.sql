with ticket_enriched as (

  select
    ticket_id,
    _fivetran_synced,
    assignee_id,
    brand_id,
    created_at,
    updated_at,
    description,
    due_at,
    group_id,
    external_id,
    is_public,
    organization_id,
    priority,
    recipient,
    requester_id,
    status,
    subject,
    problem_id,
    submitter_id,
    ticket_form_id,
    type,
    url,
    created_channel,
    source_from_id,
    source_from_title,
    source_rel,
    source_to_address,
    source_to_name,
    is_incident,
    ticket_brand_name,
    ticket_tags,
    ticket_form_name,
    ticket_total_satisfaction_scores,
    ticket_first_satisfaction_score,
    ticket_satisfaction_score,
    ticket_satisfaction_comment,
    ticket_satisfaction_reason,
    is_good_to_bad_satisfaction_score,
    is_bad_to_good_satisfaction_score,
    ticket_organization_domain_names,
    requester_organization_domain_names,
    requester_external_id,
    requester_created_at,
    requester_updated_at,
    requester_role,
    requester_email,
    requester_name,
    is_requester_active,
    requester_locale,
    requester_time_zone,
    requester_ticket_update_count,
    requester_ticket_last_update_at,
    requester_last_login_at,
    requester_organization_id,
    requester_organization_name,
    requester_organization_tags,
    requester_organization_external_id,
    requester_organization_created_at,
    requester_organization_updated_at,
    submitter_external_id,
    submitter_role,
    is_agent_submitted,
    submitter_email,
    submitter_name,
    is_submitter_active,
    submitter_locale,
    submitter_time_zone,
    assignee_external_id,
    assignee_role,
    assignee_email,
    assignee_name,
    is_assignee_active,
    assignee_locale,
    assignee_time_zone,
    assignee_ticket_update_count,
    assignee_ticket_last_update_at,
    assignee_last_login_at,
    group_name,
    organization_name,
    requester_tag,
    submitter_tag,
    assignee_tag
  from {{ ref('zendesk__ticket_enriched') }}

), ticket_resolution_times_calendar as (

  select 
    ticket_id,
    created_at,
    first_solved_at,
    last_solved_at,
    unique_assignee_count,
    assignee_stations_count,
    group_stations_count,
    first_assignee_id,
    last_assignee_id,
    first_agent_assignment_date,
    last_agent_assignment_date,
    ticket_unassigned_duration_calendar_minutes,
    total_resolutions,
    count_reopens
  from {{ ref('int_zendesk__ticket_resolution_times_calendar') }}

), ticket_reply_times_calendar as (

  select 
    ticket_id,
    first_reply_time_calendar_minutes,
    total_reply_time_calendar_minutes
  from {{ ref('int_zendesk__ticket_reply_times_calendar') }}

), ticket_comments as (

  select
    ticket_id,
    last_comment_added_at,
    count_public_agent_comments,
    count_agent_comments,
    count_end_user_comments,
    count_public_comments,
    count_internal_comments,
    total_comments,
    count_ticket_handoffs,
    is_one_touch_resolution,
    is_two_touch_resolution
  from {{ ref('int_zendesk__comment_metrics') }}

), ticket_work_time_calendar as (

  select
    ticket_id,
    last_status_assignment_date,
    ticket_deleted_count,
    agent_wait_time_in_calendar_minutes,
    requester_wait_time_in_calendar_minutes,
    agent_work_time_in_calendar_minutes,
    on_hold_time_in_calendar_minutes,
    new_status_duration_in_calendar_minutes,
    open_status_duration_in_calendar_minutes,
    total_ticket_recoveries
  from {{ ref('int_zendesk__ticket_work_time_calendar') }}

-- business hour CTEs
{% if var('using_schedules', True) %}

), ticket_first_resolution_time_business as (

  select
    ticket_id,
    first_resolution_business_minutes
  from {{ ref('int_zendesk__ticket_first_resolution_time_business') }}

), ticket_full_resolution_time_business as (

  select
    ticket_id,
    full_resolution_business_minutes
  from {{ ref('int_zendesk__ticket_full_resolution_time_business') }}

), ticket_work_time_business as (

  select
    ticket_id,
    agent_wait_time_in_business_minutes,
    requester_wait_time_in_business_minutes,
    agent_work_time_in_business_minutes,
    on_hold_time_in_business_minutes
  from {{ ref('int_zendesk__ticket_work_time_business') }}

), ticket_first_reply_time_business as (

  select
    ticket_id,
    first_reply_time_business_minutes
  from {{ ref('int_zendesk__ticket_first_reply_time_business') }}

{% endif %}
-- end business hour CTEs

), calendar_hour_metrics as (

select
    ticket_id,
    _fivetran_synced,
    assignee_id,
    brand_id,
    ticket_enriched.created_at,
    updated_at,
    description,
    due_at,
    group_id,
    external_id,
    is_public,
    organization_id,
    priority,
    recipient,
    requester_id,
    status,
    subject,
    problem_id,
    submitter_id,
    ticket_form_id,
    type,
    url,
    created_channel,
    source_from_id,
    source_from_title,
    source_rel,
    source_to_address,
    source_to_name,
    is_incident,
    ticket_brand_name,
    ticket_tags,
    ticket_form_name,
    ticket_total_satisfaction_scores,
    ticket_first_satisfaction_score,
    ticket_satisfaction_score,
    ticket_satisfaction_comment,
    ticket_satisfaction_reason,
    is_good_to_bad_satisfaction_score,
    is_bad_to_good_satisfaction_score,
    ticket_organization_domain_names,
    requester_organization_domain_names,
    requester_external_id,
    requester_created_at,
    requester_updated_at,
    requester_role,
    requester_email,
    requester_name,
    is_requester_active,
    requester_locale,
    requester_time_zone,
    requester_ticket_update_count,
    requester_ticket_last_update_at,
    requester_last_login_at,
    requester_organization_id,
    requester_organization_name,
    requester_organization_tags,
    requester_organization_external_id,
    requester_organization_created_at,
    requester_organization_updated_at,
    submitter_external_id,
    submitter_role,
    is_agent_submitted,
    submitter_email,
    submitter_name,
    is_submitter_active,
    submitter_locale,
    submitter_time_zone,
    assignee_external_id,
    assignee_role,
    assignee_email,
    assignee_name,
    is_assignee_active,
    assignee_locale,
    assignee_time_zone,
    assignee_ticket_update_count,
    assignee_ticket_last_update_at,
    assignee_last_login_at,
    group_name,
    organization_name,
    requester_tag,
    submitter_tag,
    assignee_tag,
  case when coalesce(ticket_comments.count_public_agent_comments, 0) = 0
    then null
    else ticket_reply_times_calendar.first_reply_time_calendar_minutes
      end as first_reply_time_calendar_minutes,
  case when coalesce(ticket_comments.count_public_agent_comments, 0) = 0
    then null
    else ticket_reply_times_calendar.total_reply_time_calendar_minutes
      end as total_reply_time_calendar_minutes,
  coalesce(ticket_comments.count_agent_comments, 0) as count_agent_comments,
  coalesce(ticket_comments.count_public_agent_comments, 0) as count_public_agent_comments,
  coalesce(ticket_comments.count_end_user_comments, 0) as count_end_user_comments,
  coalesce(ticket_comments.count_public_comments, 0) as count_public_comments,
  coalesce(ticket_comments.count_internal_comments, 0) as count_internal_comments,
  coalesce(ticket_comments.total_comments, 0) as total_comments,
  coalesce(ticket_comments.count_ticket_handoffs, 0) as count_ticket_handoffs, -- the number of distinct internal users who commented on the ticket
  ticket_comments.last_comment_added_at as ticket_last_comment_date,
  ticket_resolution_times_calendar.unique_assignee_count,
  ticket_resolution_times_calendar.assignee_stations_count,
  ticket_resolution_times_calendar.group_stations_count,
  ticket_resolution_times_calendar.first_assignee_id,
  ticket_resolution_times_calendar.last_assignee_id,
  ticket_resolution_times_calendar.first_agent_assignment_date,
  ticket_resolution_times_calendar.last_agent_assignment_date,
  ticket_resolution_times_calendar.first_solved_at,
  ticket_resolution_times_calendar.last_solved_at,
  case when ticket_enriched.status in ('solved', 'closed')
    then ticket_resolution_times_calendar.first_assignment_to_resolution_calendar_minutes
    else null
      end as first_assignment_to_resolution_calendar_minutes,
  case when ticket_enriched.status in ('solved', 'closed')
    then ticket_resolution_times_calendar.last_assignment_to_resolution_calendar_minutes
    else null
      end as last_assignment_to_resolution_calendar_minutes,
  ticket_resolution_times_calendar.ticket_unassigned_duration_calendar_minutes,
  ticket_resolution_times_calendar.first_resolution_calendar_minutes,
  ticket_resolution_times_calendar.final_resolution_calendar_minutes,
  ticket_resolution_times_calendar.total_resolutions as count_resolutions,
  ticket_resolution_times_calendar.count_reopens,
  ticket_work_time_calendar.ticket_deleted_count,
  ticket_work_time_calendar.total_ticket_recoveries,
  ticket_work_time_calendar.last_status_assignment_date,
  ticket_work_time_calendar.new_status_duration_in_calendar_minutes,
  ticket_work_time_calendar.open_status_duration_in_calendar_minutes,
  ticket_work_time_calendar.agent_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.requester_wait_time_in_calendar_minutes,
  ticket_work_time_calendar.agent_work_time_in_calendar_minutes,
  ticket_work_time_calendar.on_hold_time_in_calendar_minutes,
  coalesce(ticket_comments.count_agent_comments, 0) as total_agent_replies,
  
  case when ticket_enriched.is_requester_active = true and ticket_enriched.requester_last_login_at is not null
    then ({{ dbt_utils.datediff("ticket_enriched.requester_last_login_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as requester_last_login_age_minutes,
  case when ticket_enriched.is_assignee_active = true and ticket_enriched.assignee_last_login_at is not null
    then ({{ dbt_utils.datediff("ticket_enriched.assignee_last_login_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as assignee_last_login_age_minutes,
  case when lower(ticket_enriched.status) not in ('solved','closed')
    then ({{ dbt_utils.datediff("ticket_enriched.created_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as unsolved_ticket_age_minutes,
  case when lower(ticket_enriched.status) not in ('solved','closed')
    then ({{ dbt_utils.datediff("ticket_enriched.updated_at", dbt_utils.current_timestamp(), 'second') }} /60)
      end as unsolved_ticket_age_since_update_minutes,
  case when lower(ticket_enriched.status) in ('solved','closed') and ticket_comments.is_one_touch_resolution 
    then true
    else false
      end as is_one_touch_resolution,
  case when lower(ticket_enriched.status) in ('solved','closed') and ticket_comments.is_two_touch_resolution 
    then true
    else false 
      end as is_two_touch_resolution,
  case when lower(ticket_enriched.status) in ('solved','closed') and not ticket_comments.is_one_touch_resolution
      and not ticket_comments.is_two_touch_resolution 
    then true
    else false 
      end as is_multi_touch_resolution


from ticket_enriched

left join ticket_reply_times_calendar
  using (ticket_id)

left join ticket_resolution_times_calendar
  using (ticket_id)

left join ticket_work_time_calendar
  using (ticket_id)

left join ticket_comments
  using(ticket_id)

{% if var('using_schedules', True) %}

), business_hour_metrics as (

  select 
    ticket_enriched.ticket_id,
    ticket_first_resolution_time_business.first_resolution_business_minutes,
    ticket_full_resolution_time_business.full_resolution_business_minutes,
    ticket_first_reply_time_business.first_reply_time_business_minutes,
    ticket_work_time_business.agent_wait_time_in_business_minutes,
    ticket_work_time_business.requester_wait_time_in_business_minutes,
    ticket_work_time_business.agent_work_time_in_business_minutes,
    ticket_work_time_business.on_hold_time_in_business_minutes

  from ticket_enriched

  left join ticket_first_resolution_time_business
    using (ticket_id)

  left join ticket_full_resolution_time_business
    using (ticket_id)
  
  left join ticket_first_reply_time_business
    using (ticket_id)  
  
  left join ticket_work_time_business
    using (ticket_id)

)

select
    ticket_id,
    _fivetran_synced,
    assignee_id,
    brand_id,
    created_at,
    updated_at,
    description,
    due_at,
    group_id,
    external_id,
    is_public,
    organization_id,
    priority,
    recipient,
    requester_id,
    status,
    subject,
    problem_id,
    submitter_id,
    ticket_form_id,
    type,
    url,
    created_channel,
    source_from_id,
    source_from_title,
    source_rel,
    source_to_address,
    source_to_name,
    is_incident,
    ticket_brand_name,
    ticket_tags,
    ticket_form_name,
    ticket_total_satisfaction_scores,
    ticket_first_satisfaction_score,
    ticket_satisfaction_score,
    ticket_satisfaction_comment,
    ticket_satisfaction_reason,
    is_good_to_bad_satisfaction_score,
    is_bad_to_good_satisfaction_score,
    ticket_organization_domain_names,
    requester_organization_domain_names,
    requester_external_id,
    requester_created_at,
    requester_updated_at,
    requester_role,
    requester_email,
    requester_name,
    is_requester_active,
    requester_locale,
    requester_time_zone,
    requester_ticket_update_count,
    requester_ticket_last_update_at,
    requester_last_login_at,
    requester_organization_id,
    requester_organization_name,
    requester_organization_tags,
    requester_organization_external_id,
    requester_organization_created_at,
    requester_organization_updated_at,
    submitter_external_id,
    submitter_role,
    is_agent_submitted,
    submitter_email,
    submitter_name,
    is_submitter_active,
    submitter_locale,
    submitter_time_zone,
    assignee_external_id,
    assignee_role,
    assignee_email,
    assignee_name,
    is_assignee_active,
    assignee_locale,
    assignee_time_zone,
    assignee_ticket_update_count,
    assignee_ticket_last_update_at,
    assignee_last_login_at,
    group_name,
    organization_name,
    requester_tag,
    submitter_tag,
    assignee_tag,
    total_reply_time_calendar_minutes,
    count_agent_comments,
    count_public_agent_comments,
    count_end_user_comments,
    count_public_comments,
    count_internal_comments,
    total_comments,
    count_ticket_handoffs,
    ticket_last_comment_date,
    unique_assignee_count,
    assignee_stations_count,
    group_stations_count,
    first_assignee_id,
    last_assignee_id,
    first_agent_assignment_date,
    last_agent_assignment_date,
    first_solved_at,
    last_solved_at,
    first_assignment_to_resolution_calendar_minutes,
    last_assignment_to_resolution_calendar_minutes,
    ticket_unassigned_duration_calendar_minutes,
    first_resolution_calendar_minutes,
    final_resolution_calendar_minutes,
    count_resolutions,
    count_reopens,
    ticket_deleted_count,
    total_ticket_recoveries,
    last_status_assignment_date,
    new_status_duration_in_calendar_minutes,
    open_status_duration_in_calendar_minutes,
    agent_wait_time_in_calendar_minutes,
    requester_wait_time_in_calendar_minutes,
    agent_work_time_in_calendar_minutes,
    on_hold_time_in_calendar_minutes,
    total_agent_replies,
    requester_last_login_age_minutes,
    assignee_last_login_age_minutes,
    unsolved_ticket_age_minutes,
    unsolved_ticket_age_since_update_minutes,
    is_one_touch_resolution,
    is_two_touch_resolution,
    is_multi_touch_resolution,
  case when calendar_hour_metrics.status in ('solved', 'closed')
    then business_hour_metrics.first_resolution_business_minutes
    else null
      end as first_resolution_business_minutes,
  case when calendar_hour_metrics.status in ('solved', 'closed')
    then business_hour_metrics.full_resolution_business_minutes
    else null
      end as full_resolution_business_minutes,
  case when coalesce(calendar_hour_metrics.count_public_agent_comments, 0) = 0
    then null
    else business_hour_metrics.first_reply_time_business_minutes
      end as first_reply_time_business_minutes,
  business_hour_metrics.agent_wait_time_in_business_minutes,
  business_hour_metrics.requester_wait_time_in_business_minutes,
  business_hour_metrics.agent_work_time_in_business_minutes,
  business_hour_metrics.on_hold_time_in_business_minutes

from calendar_hour_metrics

left join business_hour_metrics 
  using (ticket_id)

{% else %}

) 

select
    ticket_id,
    _fivetran_synced,
    assignee_id,
    brand_id,
    created_at,
    updated_at,
    description,
    due_at,
    group_id,
    external_id,
    is_public,
    organization_id,
    priority,
    recipient,
    requester_id,
    status,
    subject,
    problem_id,
    submitter_id,
    ticket_form_id,
    type,
    url,
    created_channel,
    source_from_id,
    source_from_title,
    source_rel,
    source_to_address,
    source_to_name,
    is_incident,
    ticket_brand_name,
    ticket_tags,
    ticket_form_name,
    ticket_total_satisfaction_scores,
    ticket_first_satisfaction_score,
    ticket_satisfaction_score,
    ticket_satisfaction_comment,
    ticket_satisfaction_reason,
    is_good_to_bad_satisfaction_score,
    is_bad_to_good_satisfaction_score,
    ticket_organization_domain_names,
    requester_organization_domain_names,
    requester_external_id,
    requester_created_at,
    requester_updated_at,
    requester_role,
    requester_email,
    requester_name,
    is_requester_active,
    requester_locale,
    requester_time_zone,
    requester_ticket_update_count,
    requester_ticket_last_update_at,
    requester_last_login_at,
    requester_organization_id,
    requester_organization_name,
    requester_organization_tags,
    requester_organization_external_id,
    requester_organization_created_at,
    requester_organization_updated_at,
    submitter_external_id,
    submitter_role,
    is_agent_submitted,
    submitter_email,
    submitter_name,
    is_submitter_active,
    submitter_locale,
    submitter_time_zone,
    assignee_external_id,
    assignee_role,
    assignee_email,
    assignee_name,
    is_assignee_active,
    assignee_locale,
    assignee_time_zone,
    assignee_ticket_update_count,
    assignee_ticket_last_update_at,
    assignee_last_login_at,
    group_name,
    organization_name,
    requester_tag,
    submitter_tag,
    assignee_tag,
    total_reply_time_calendar_minutes,
    count_agent_comments,
    count_public_agent_comments,
    count_end_user_comments,
    count_public_comments,
    count_internal_comments,
    total_comments,
    count_ticket_handoffs,
    ticket_last_comment_date,
    unique_assignee_count,
    assignee_stations_count,
    group_stations_count,
    first_assignee_id,
    last_assignee_id,
    first_agent_assignment_date,
    last_agent_assignment_date,
    first_solved_at,
    last_solved_at,
    first_assignment_to_resolution_calendar_minutes,
    last_assignment_to_resolution_calendar_minutes,
    ticket_unassigned_duration_calendar_minutes,
    first_resolution_calendar_minutes,
    final_resolution_calendar_minutes,
    count_resolutions,
    count_reopens,
    ticket_deleted_count,
    total_ticket_recoveries,
    last_status_assignment_date,
    new_status_duration_in_calendar_minutes,
    open_status_duration_in_calendar_minutes,
    agent_wait_time_in_calendar_minutes,
    requester_wait_time_in_calendar_minutes,
    agent_work_time_in_calendar_minutes,
    on_hold_time_in_calendar_minutes,
    total_agent_replies,
    requester_last_login_age_minutes,
    assignee_last_login_age_minutes,
    unsolved_ticket_age_minutes,
    unsolved_ticket_age_since_update_minutes,
    is_one_touch_resolution,
    is_two_touch_resolution,
    is_multi_touch_resolution
from calendar_hour_metrics

{% endif %}
