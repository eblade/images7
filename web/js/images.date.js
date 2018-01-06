$(function() {
    var load = function(params) {
        params = params || Object();
        var date = params.date || 'today';
        var delta = params.delta || '0';
        var override = params.override || false;
        $.Images.Viewer.focus = 0;
        if (document.location.hash !== '#' + date) {
            document.location.hash = '#' + date;
        } else if (!override) {
            return;
        }
        $.ajax({
            url: 'entry?date=' + date + '&delta=' + delta,
            success: function(data) {
                date = data.date;
                $.Images.Viewer.date = date;
                $.Images.Viewer.index_hash = date;
                $('#viewer_feed')
                    .html('');
                $('#date_this')
                    .html(date)
                    .click(function() { load_date({'date': 'today', 'delta': '0'}); });
                $.Images.autosave('#date_info_short');
                $.Images.autosave('#date_info_full');
                load_date_info(date);
                $.each(data.entries, $.Images.Viewer.add_thumb);
                $.Images.Viewer.update_focus({focus: 0});
                $.Images.Viewer.total_count = data.count;
            },
        });
    };

    var load_date_info = function (date) {
        date = date || $.Images.Viewer.date;
        $.ajax({
            url: '/date/' + date,
            success: function(data) {
                $('#date_info_short').val(data.short);
                $('#date_info_short')[0].setAttribute('data-url', '/date/' + date);
                $('#date_info_full').html(data.full);
                $('#date_info_full')[0].setAttribute('data-url', '/date/' + date);
                $('#date_details')
                    .html(data.count + (data.count === 1 ? ' entry' : ' entries'));
                if (data.stats) {
                    if (data.stats.pending > 0) {
                        $('#date_details')
                            .append('<span class="viewer_state_label toggle_state_pending">' + data.stats.pending + ' pending</span>');
                    }
                    if (data.stats.purge > 0) {
                        $('#date_details')
                            .append('<span class="viewer_state_label toggle_state_purge">' + data.stats.purge + ' to purge</span>');
                    }
                    if (data.stats.todo > 0) {
                        $('#date_details')
                            .append('<span class="viewer_state_label toggle_state_todo">' + data.stats.todo + ' todo</span>');
                    }
                    if (data.stats.wip > 0) {
                        $('#date_details')
                            .append('<span class="viewer_state_label toggle_state_wip">' + data.stats.wip + ' wip</span>');
                    }
                    if (data.stats.final > 0) {
                        $('#date_details')
                            .append('<span class="viewer_state_label toggle_state_final">' + data.stats.final + ' final</span>');
                    }
                }
            },
            error: function(data) {
            },
        })
    };

    $(document)
        .ready(function() {
            $.Images.Viewer.bind_keys();
            $.Images.Viewer.state_callback = load_date_info;
            $(window)
                .hashchange(function() {
                    load({
                        date: document.location.hash.substr(1) || 'today',
                    });
                });
            load({
                date: document.location.hash.substr(1) || 'today',
                override: true,
            });
        });
});
