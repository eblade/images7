$(function() {
    var jobs = function (menu, id) {
        var load = function (force) {
            $.ajax({
                url: 'job?page_size=20',
                method: 'GET',
                success: function (data) {
                    if (data.stats.failed === 0) {
                        var enqueued = data.stats.new + data.stats.acquired + data.stats.active;
                        $(id).removeClass('error').html(enqueued ? enqueued + ' jobs' : 'system');
                        if ((data.stats.active + data.stats.acquired + data.stats.new) > 0) {
                            $(id).addClass('active');
                        } else {
                            $(id).removeClass('active');
                        }
                    } else {
                        $(id).removeClass('active').addClass('error').html(data.stats.error);
                    }
                    if (menu.showing('jobs') || force) {
                        $(menu.container_id).html(
                                '<table id="job_table"><thead>' +
                                '<th></th>' +
                                '<th>method</th>' +
                                '<th>updated</th>' +
                                '<th>time</th>' +
                                '<th>entry</th>' +
                                '<th>comment</th>' +
                                '</thead><tbody></tbody></table>');

                        $.each(data.entries, function(index, entry) {
                            var comment = '';
                            if (entry.method === 'jpeg_import') {
                                if (entry.options.is_derivative) {
                                    comment = 'Attach ' + entry.options.folder + '/' + entry.options.source_path;
                                } else {
                                    comment = 'Import ' + entry.options.folder + '/' + entry.options.source_path;
                                }
                            } else if (entry.method === 'raw_import') {
                                comment = 'Import ' + entry.options.folder + '/' + entry.options.source_path;
                            } else if (entry.method === 'imageproxy') {
                                comment = 'Generate proxy';
                            } else if (entry.method === 'rules') {
                                comment = 'Check rules';
                            } else if (entry.method === 'remote') {
                                comment = 'Remote transfer';
                            } else if (entry.method === 'delete') {
                                comment = 'Delete variant ' + entry.options.variant.purpose + '/'
                                    + entry.options.variant.version;
                            }
                            var updated = new Date(entry.updated * 1000);
                            var time = '-';
                            if (entry.stopped && entry.started) {
                                time = (entry.stopped - entry.started).toFixed(3);
                            }
                            $('#job_table tbody')
                                .append('<tr>' +
                                        '<td class="state ' + entry.state + '">' + entry.state.substr(0, 1) + '</td>' +
                                        '<td class="method">' + entry.method + '</td>' +
                                        '<td class="updated">' + updated + '</td>' +
                                        '<td class="time">' + time + '</td>' +
                                        '<td class="entry">' + (entry.options.entry_id || '-') + '</td>' +
                                        '<td class="comment">' + (entry.message || comment) + '</td>' +
                                        '</tr>');
                        });

                        $(menu.container_id)
                            .append('<div class="job_info">' +
                                    data.stats.total + ' jobs (' +
                                    data.stats.new + ' new, ' +
                                    data.stats.acquired + ' acquired, ' +
                                    data.stats.active + ' active, ' +
                                    data.stats.held + ' held, ' +
                                    data.stats.done + ' done and ' +
                                    data.stats.failed + ' failed)</div>' +
                                    '<div class="job_action_buttons">' +
                                    '<div class="job_action_button" ' +
                                    'id="delete_jobs_button">clear all</div></div>');

                        $('#delete_jobs_button')
                            .click(function() {
                                $.ajax({
                                    url: '/job',
                                    method: 'DELETE',
                                    success: function(data) {},
                                    error: function(data) {
                                        $(id).addClass('error').html('ERROR');
                                    },
                                });
                            });
                    }
                },
                error: function (data) {
                    $(id).addClass('error').html('ERROR');
                },
            });
        };

        load(true);
        window.setInterval(load, 5000);

        menu.register_button('jobs', id);
        $(id).click(function(e) {
            menu.toggle('jobs', load);
        });
    };

    $.Images = $.Images || {};
    $.Images.jobs = jobs;
});
