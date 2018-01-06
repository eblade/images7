$(function() {
    var
        feed_div = '#viewer_feed';

    $.Images = $.Images || {};
    $.Images.Viewer = {
        offset: 0,
        focus: 0,
        showing_viewer: false,
        showing_metadata: false,
        showing_copies: false,
        mode: 'proxy',
        date: null,
        state_callback: function() {},
        index_hash: '',
    };

    var update_focus = function(params) {
        params = params || new Object();
        var move = params.move || 0;
        var focus = params.focus;
        var old_focus = $.Images.Viewer.focus;

        if (focus === undefined) {
            $.Images.Viewer.focus += move;
        } else if (focus === -1) {
            $.Images.Viewer.focus = $.Images.Viewer.total_count - 1;
        } else {
            $.Images.Viewer.focus = focus;
        }

        if ($.Images.Viewer.focus < 0) {
            $.Images.Viewer.focus = 0;
        } else if ($.Images.Viewer.focus >= $.Images.Viewer.total_count) {
            $.Images.Viewer.focus = $.Images.Viewer.total_count - 1;
        }

        if ($.Images.Viewer.focus !== old_focus) {
            $.scroll_to('#thumb_' + $.Images.Viewer.focus, 260);
        }

        $('.thumb').each(function(index, thumb) {
            if (index === $.Images.Viewer.focus) {
                if ($.Images.Viewer.mode === 'check') {
                    $.Images.Viewer.mode = 'proxy';
                }
                if ($.Images.Viewer.showing_viewer) {
                    show_viewer({
                        proxy_url: thumb.getAttribute('data-proxy-url'),
                        strip: $.Images.Viewer.fix_strip(thumb.getAttribute('data-strip')),
                        animate: false,
                    });
                }
                $(thumb).addClass('thumb_focused');
                $('#day_view').animate({
                    top: -$(thumb).position().top + 270,
                }, 200);
            } else {
                $(thumb).removeClass('thumb_focused');
            }
        });
    };

    var toggle_select = function() {
        $('.thumb:eq(' + $.Images.Viewer.focus + ')').toggleClass('thumb_selected');
    };

    var purge_pending = function() {
        $('.thumb').each(function(index, thumb) {
            var state = thumb.getAttribute('data-state');
            if (state == 'pending') {
                var url = thumb.getAttribute('data-state-url');
                $.ajax({
                    url: url + '?state=purge&soft=yes',
                    method: 'PUT',
                    success: function(data) {
                        thumb.setAttribute('data-state', 'purge');
                        $.Images.Viewer.state_callback();
                    },
                    error: function(data) {
                        alert("Unabled to purge " + id);
                    },
                });
                $(thumb).removeClass('state_pending');
                $(thumb).addClass('state_purge');
            }
        });
        $('#day_thisday').click();
    };

    var show_viewer = function(params) {
        params = params || Object();
        var proxy_url = params.proxy_url;
        var strip = params.strip || '';
        var animate = params.animate === undefined ? true : params.animate;

        $('#viewer_metadata').hide();
        $('#viewer_copies').hide();

        $('#viewer_overlay').css('background-image', 'url(' + proxy_url + ')');
        if (animate) {
            $('#viewer_overlay').fadeIn();
        }
        $('#viewer_strip').html(strip);
        $.Images.Viewer.mode = 'proxy';
        $.Images.Viewer.showing_viewer = true;
        $.Images.Viewer.showing_metadata = false;
        $.Images.Viewer.showing_copies = false;
        sync_overlay_buttons();
    };

    var back_to_index = function() {
        document.location = '/#' + $.Images.Viewer.index_hash;
    };

    var hide_viewer = function() {
        $.Images.Viewer.state_callback();
        $.Images.Viewer.showing_viewer = false;
        $('#viewer_overlay').hide();
    };

    var sync_overlay_buttons = function() {
        var thumb = $('img.thumb')[$.Images.Viewer.focus];
        var state = thumb.getAttribute('data-state');
        if (state === 'keep') {
            $('#viewer_keep').addClass('toggle_state_keep');
        } else {
            $('#viewer_keep').removeClass('toggle_state_keep');
        }
        if (state === 'purge') {
            $('#viewer_purge').addClass('toggle_state_purge');
        } else {
            $('#viewer_purge').removeClass('toggle_state_purge');
        }
        if (state === 'todo') {
            $('#viewer_todo').addClass('toggle_state_todo');
        } else {
            $('#viewer_todo').removeClass('toggle_state_todo');
        }
        if (state === 'wip') {
            $('#viewer_wip').addClass('toggle_state_wip');
        } else {
            $('#viewer_wip').removeClass('toggle_state_wip');
        }
        if (state === 'final') {
            $('#viewer_final').addClass('toggle_state_final');
        } else {
            $('#viewer_final').removeClass('toggle_state_final');
        }
        if ($.Images.Viewer.mode === 'check') {
            $('#viewer_check').addClass('toggle_selected');
        } else {
            $('#viewer_check').removeClass('toggle_selected');
        }
        if ($.Images.Viewer.showing_metadata) {
            $('#viewer_toggle_metadata').addClass('toggle_selected').addClass('toggle_extended');
        } else {
            $('#viewer_toggle_metadata').removeClass('toggle_selected').removeClass('toggle_extended');
        }
        if ($.Images.Viewer.showing_copies) {
            $('#viewer_toggle_copies').addClass('toggle_selected').addClass('toggle_extended');
        } else {
            $('#viewer_toggle_copies').removeClass('toggle_selected').removeClass('toggle_extended');
        }
    };

    var set_state = function(id, new_state) {
        var thumb = $('img.thumb')[id];
        var url = thumb.getAttribute('data-state-url');
        $.ajax({
            url: url + '?state=' + new_state,
            method: 'PUT',
            success: function(data) {
                thumb.setAttribute('data-state', new_state);
                sync_overlay_buttons();
                $(thumb).removeClass('state_pending');
                $(thumb).removeClass('state_purge');
                $(thumb).removeClass('state_keep');
                $(thumb).removeClass('state_todo');
                $(thumb).removeClass('state_wip');
                $(thumb).removeClass('state_final');
                $(thumb).addClass('state_' + new_state);
            },
            error: function(data) {
                alert("Unabled to set state for " + id);
            },
        });
    };

    var toggle_check = function() {
        var thumb = $('img.thumb')[$.Images.Viewer.focus];
        if ($.Images.Viewer.mode === 'check') {
            var url = thumb.getAttribute('data-proxy-url');
            $.Images.Viewer.mode = 'proxy';
        } else {
            var url = thumb.getAttribute('data-check-url');
            $.Images.Viewer.mode = 'check';
        }
        $('#viewer_overlay').css('background-image', 'url(' + url + ')');
        sync_overlay_buttons();
    };

    var toggle_metadata = function(showing_metadata, and_focus_input) {
        if (showing_metadata === undefined) {
            $.Images.Viewer.showing_metadata = !$.Images.Viewer.showing_metadata;
        } else {
            $.Images.Viewer.showing_metadata = showing_metadata;
        }
        if ($.Images.Viewer.showing_metadata) {
            $.Images.Viewer.showing_copies = false;
            $('#viewer_copies').fadeOut();
        }
        sync_overlay_buttons();
        if ($.Images.Viewer.showing_metadata) {
            var thumb = $('img.thumb')[$.Images.Viewer.focus];
            var url = thumb.getAttribute('data-self-url');
            $('#viewer_metadata').fadeIn();
            $('#viewer_metadata_content').html('loading...');
            $.ajax({
                url: url,
                success: function(data) {
                    $('#viewer_metadata_content')
                        .html('');
                    var row = function(key, value) {
                        value = value || '-';
                        $('#viewer_metadata_content')
                            .append('<div class="viewer_metadata_key">' + key + '</div>' +
                                    '<div class="viewer_metadata_value">' + value + '</div>');
                    };
                    var srow = function(key, value, patch_key, in_metadata, valid_values) {
                        value = value === null ? '' : value;
                        var
                            url = '/entry/' + data._id + (in_metadata ? '/metadata' : ''),
                            id = 'autosave_' + patch_key;
                        $('#viewer_metadata_content')
                            .append('<div class="viewer_metadata_key">' + key + '</div>' +
                                    '<div class="viewer_metadata_editable">' +
                                    '<input id="' + id + '" value="' + value + '" ' +
                                           'data-url="' + url + '" ' +
                                           'data-name="' + patch_key + '"/></div>');
                        $.Images.autosave('#' + id, valid_values !== undefined ? function(value) {
                            return valid_values.indexOf(value) !== -1;
                        } : undefined);
                    };
                    var mrow = function(key, value, patch_key) {
                        value = value === null ? '' : value.join(', ');
                        var
                            url = '/entry/' + data._id,
                            id = 'autosave_' + patch_key;
                        $('#viewer_metadata_content')
                            .append('<div class="viewer_metadata_key">' + key + '</div>' +
                                    '<div class="viewer_metadata_editable">' +
                                    '<input id="' + id + '" value="' + value + '" ' +
                                           'data-url="' + url + '" ' +
                                           'data-name="' + patch_key + '"/></div>');
                        $.Images.autosave('#' + id, undefined, function(value) {
                            value = value.split(',');
                            value = value.map(function(x) { return x.trim() });
                            return value.filter(function(x) { return x !== "" });
                        });
                    };
                    row('ID', data._id);
                    srow('Title', data.title, 'title', false);
                    srow('Description', data.description, 'description', false);
                    mrow('Tags', data.tags, 'tags');
                    srow('Artist', data.metadata.Artist, 'Artist', true);
                    row('Taken', data.metadata.DateTimeOriginal);
                    if (data.metadata.FNumber[0] === 0) {
                        row('Aperture', 'unknown');
                    } else {
                        row('Aperture', 'f' + data.metadata.FNumber[0] + '/' + data.metadata.FNumber[1]);
                    }
                    row('ISO', data.metadata.ISOSpeedRatings);
                    row('Focal length', data.metadata.FocalLength[0] + '/' + data.metadata.FocalLength[1]);
                    row('35mm equiv.', data.metadata.FocalLengthIn35mmFilm);
                    row('Geometry', data.metadata.Geometry[0] + ' x ' + data.metadata.Geometry[1]);
                    row('Exposure', data.metadata.ExposureTime[0] + '/' + data.metadata.ExposureTime[1]);
                    srow('Angle', data.metadata.Angle, 'Angle', true, ['-270', '-180', '-90', '0', '90', '180', '270']);
                    srow('Copyright', data.metadata.Copyright, 'Copyright', true);
                    row('Source', data.import_folder + '/' + data.original_filename);
                    if (and_focus_input !== undefined) {
                        $('#autosave_' + and_focus_input).focus();
                    }
                },
                error: function(data) {
                    $('#viewer_metadata_content').html('no metadata, apparently');
                },
            });
        } else {
            $('#viewer_metadata').fadeOut(300);
            $.wait(300).then(function() {
                $('#viewer_metadata_content').html('');
            });
        }

    };

    var toggle_copies = function() {
        $.Images.Viewer.showing_copies = !$.Images.Viewer.showing_copies;
        if ($.Images.Viewer.showing_copies) {
            $.Images.Viewer.showing_metadata = false;
            $('#viewer_metadata').fadeOut();
        }
        sync_overlay_buttons();
        if ($.Images.Viewer.showing_copies) {
            var thumb = $('img.thumb')[$.Images.Viewer.focus];
            var url = thumb.getAttribute('data-self-url');
            $('#viewer_copies').fadeIn();
            $('#viewer_copies_content').html('loading...');
            $.ajax({
                url: url,
                success: function(data) {
                    $('#viewer_copies_content').html('<table id="copies"><thead>' +
                                                     '<th>store</th>' +
                                                     '<th>purpose</th>' +
                                                     '<th>source</th>' +
                                                     '<th>mimetype</th>' +
                                                     '<th>size</th>' +
                                                     '<th>geometry</th>' +
                                                     '<th>angle</th>' +
                                                     '<th>mirror</th>' +
                                                     '<th></th>' +
                                                     '</thead><tbody></tbody><table>');

                    var has_original = false,
                        has_derivative = false,
                        has_proxy = false,
                        has_raw = false,
                        has_flickr = false;

                    var action = function(key, title, callback) {
                        var id = 'action_' + key;
                        $('#viewer_copies_content')
                            .append('<div class="viewer_action_button" id="' + id + '">' + title + '</div>');

                        if (callback) {
                            $('#' + id).click(callback);
                        }
                    };

                    var header = function(title) {
                        $('#viewer_copies_content')
                            .append('<h2>' + title + '</h2>');
                    }

                    var link = function(v, url) {
                        var
                            source = '',
                            geometry = '',
                            angle = '',
                            mirror = '',
                            show = '';
                        if (v.source_purpose !== null) {
                            source = v.source_purpose + '/' + v.source_version;
                        }
                        if (v.height !== null) {
                            geometry = v.height + 'x' + v.width;
                        }
                        if (v.purpose === 'proxy' || v.purpose === 'check') {
                            var id = 'use_' + v.purpose + '_' + v.version;
                            show = '<span class="viewer_show" id="' + id + '">&gt;&gt;&gt;</span>';
                        }
                        if (v.angle !== null) {
                            angle = v.angle;
                        }
                        if (v.mirror !== null) {
                            mirror = v.mirror;
                        }
                        $('#copies tbody')
                            .append('<tr>' +
                                    '<td>' + v.store + '</td>' +
                                    '<td><a href="' + url + '">' + v.purpose + '/' + v.version + '</a></td>' +
                                    '<td>' + source + '</td>' +
                                    '<td>' + (v.mime_type || 'unknown') + '</td>' +
                                    '<td>' + (v.size || '?') + '</td>' +
                                    '<td>' + geometry + '</td>' +
                                    '<td>' + angle + '</td>' +
                                    '<td>' + mirror + '</td>' +
                                    '<td>' + show + '</td>' +
                                    '</tr>');
                        if (v.purpose === 'proxy' || v.purpose === 'check') {
                            var id = 'use_' + v.purpose + '_' + v.version;
                            $('#' + id)
                                .click(function() {
                                    toggle_copies();
                                    $('#viewer_overlay').css('background-image', 'url(' + url + ')');
                                    $.Images.Viewer.mode = v.purpose;
                                    sync_overlay_buttons();
                                });
                        }
                    }

                    var backup_row = function(b) {
                        if (b.source_purpose !== null) {
                            source = b.source_purpose + '/' + b.source_version;
                        }
                        $('#copies tbody')
                            .append('<tr>' +
                                    '<td>' + b.method + '</td>' +
                                    '<td>' + b.key + '</td>' +
                                    '<td>' + source + '</td>' +
                                    '<td></td>' +
                                    '<td></td>' +
                                    '<td></td>' +
                                    '</tr>');
                    };

                    $.each(data.variants, function(index, variant) {
                        if (variant.purpose === 'original') { has_original = true; }
                        if (variant.purpose === 'proxy') { has_proxy = true; }
                        if (variant.purpose === 'raw') { has_raw = true; }
                        if (variant.purpose === 'derivative') { has_derivative = true; }

                        link(variant, data.urls[variant.purpose][variant.version] + "?download=yes");
                    });

                    if (has_original || has_derivative) {
                        action('proxy', 'create new proxy', function() {
                            $.ajax({
                                url: '/job',
                                method: 'POST',
                                contentType: "application/json",
                                data: JSON.stringify({
                                    '*schema': 'Job',
                                    method: 'imageproxy',
                                    options: {
                                        '*schema': 'ImageProxyOptions',
                                        entry_id: data._id,
                                        source_purpose: has_derivative ? 'derivative' : 'original',
                                    },
                                }),
                                success: function(data) {
                                    $('#action_proxy').html('creating');
                                },
                                error: function(data) {
                                    $('#action_proxy').html('error');
                                },
                            });
                        });
                    }
                    if (!has_raw) {
                        action('raw', 'fetch raw file', function() {
                            $.ajax({
                                url: '/job',
                                method: 'POST',
                                contentType: "application/json",
                                data: JSON.stringify({
                                    '*schema': 'Job',
                                    method: 'rawfetch',
                                    options: {
                                        '*schema': 'RawFetchOptions',
                                        entry_id: data._id,
                                    },
                                }),
                                success: function(data) {
                                    $('#action_raw').html('fetching');
                                },
                                error: function(data) {
                                    $('#action_raw').html('error');
                                },
                            });
                        });
                    }

                    $.each(data.backups, function(index, backup) {
                        if (backup.method === 'flickr') {
                            has_flickr = true;
                        }
                        backup_row(backup);
                    });

                    action('flickr', has_flickr ? 'replace on flickr' : 'publish to flickr', function() {
                        $.ajax({
                            url: '/job',
                            method: 'POST',
                            contentType: "application/json",
                            data: JSON.stringify({
                                '*schema': 'Job',
                                method: 'flickr',
                                options: {
                                    '*schema': 'FlickrOptions',
                                    entry_id: data._id,
                                    source_purpose: has_derivative ? 'derivative' : 'original',
                                },
                            }),
                            success: function(data) {
                                $('#action_flickr').html('sent');
                            },
                            error: function(data) {
                                $('#action_flickr').html('error');
                            },
                        });
                    });

                    action('amend', 'amend entry', function() {
                        $.ajax({
                            url: '/job',
                            method: 'POST',
                            contentType: "application/json",
                            data: JSON.stringify({
                                '*schema': 'Job',
                                method: 'amend',
                                options: {
                                    '*schema': 'AmendOptions',
                                    entry_id: data._id,
                                },
                            }),
                            success: function(data) {
                                $('#action_amend').fadeOut(400);
                            },
                            error: function(data) {
                                $('#action_amend').html('error');
                            },
                        });
                    });

                    action('rules', 'check rules', function() {
                        $.ajax({
                            url: '/job',
                            method: 'POST',
                            contentType: "application/json",
                            data: JSON.stringify({
                                '*schema': 'Job',
                                method: 'rules',
                                options: {
                                    '*schema': 'RulesOptions',
                                    entry_id: data._id,
                                },
                            }),
                            success: function(data) {
                                $('#action_rules').fadeOut(400);
                            },
                            error: function(data) {
                                $('#action_rules').html('error');
                            },
                        });
                    });
                },
                error: function(data) {
                    $('#viewer_copies_content').html('Error!');
                },
            });
        } else {
            $('#viewer_copies').fadeOut();
        }

    };

    var create_strip = function(entry) {
        return entry.taken_ts + ' #' + entry._id + ' '
            + entry.import_folder + '/' + entry.original_filename
            + ' [' + entry.mime_type + '] '
            + entry.tags
                .map(function(s) { return '[tag]' + s + '[/tag]'; })
                .join(' ');
    };

    var fix_strip = function(s) {
        return s
            .replace(/\[tag\]/g, '<span class="viewer_strip_tag">')
            .replace(/\[\/tag\]/g, '</span>')
    }

    var add_thumb = function(index, entry) {
        $('#viewer_feed')
            .append('<img data-self-url="' + entry.self_url +
                    '" data-id="' + entry._id +
                    '" data-state="' + entry.state +
                    '" data-state-url="' + entry.state_url +
                    '" data-check-url="' + entry.check_url +
                    '" data-proxy-url="' + entry.proxy_url +
                    '" data-strip="' + create_strip(entry) +
                    '" class="thumb state_' + entry.state + '" id="thumb_' + index +
                    '" src="' + entry.thumb_url +
                    '" title="' + index + '"/>');
        $('#thumb_' + index)
            .click(function(event) {
                var id = parseInt(this.title);
                var thumb = $('img.thumb')[id]
                update_focus({focus: id});
                show_viewer({
                    proxy_url: thumb.getAttribute('data-proxy-url'),
                    strip: fix_strip(thumb.getAttribute('data-strip')),
                });
            });
    };

    $('#viewer_back').click(back_to_index);
    $('#viewer_autopurge').click(purge_pending);

    var select_all = function() {
        $('.thumb').each(function(index, thumb) {
            $(thumb).addClass('thumb_selected');
        });
    };
    $('#viewer_select_all').click(select_all);

    var select_none = function() {
        $('.thumb').each(function(index, thumb) {
            $(thumb).removeClass('thumb_selected');
        });
    };
    $('#viewer_select_none').click(select_none);

    $('#viewer_keep').click(function() { set_state($.Images.Viewer.focus, 'keep'); });
    $('#viewer_purge').click(function() { set_state($.Images.Viewer.focus, 'purge'); });
    $('#viewer_todo').click(function() { set_state($.Images.Viewer.focus, 'todo'); });
    $('#viewer_wip').click(function() { set_state($.Images.Viewer.focus, 'wip'); });
    $('#viewer_final').click(function() { set_state($.Images.Viewer.focus, 'final'); });
    $('#viewer_toggle_metadata').click(function() { toggle_metadata(); });
    $('#viewer_toggle_copies').click(function() { toggle_copies(); });
    $('#viewer_check').click(function() { toggle_check(); });
    $('#viewer_close').click(function() { hide_viewer(); });

    $('#viewer_input').hide();

    var show_bulk_add_tags = function() {
        $('#viewer_input').fadeIn();
        $('#viewer_input_heading').html('Add tags');
        $('#viewer_input_box').focus()
        $.wait(100).then(function() {
            $('#viewer_input_box').val('');
        });
        $.Images.Viewer.showing_input_box = true;
        $.Images.Viewer.input_box_callback = function() {
            var value = $('#viewer_input_box').val();
            value = value.split(',');
            value = value.map(function(x) { return x.trim() });
            value = value.filter(function(x) { return x !== "" });
            $.ajax({
                url: '/job',
                method: 'POST',
                contentType: "application/json",
                data: JSON.stringify({
                    '*schema': 'Job',
                    method: 'tag_update',
                    options: {
                        '*schema': 'TagBulkUpdateOptions',
                        entry_ids: $.map($('.thumb_selected'), function(thumb) { return thumb.getAttribute('data-id'); }),
                        add_tags: value,
                    },
                }),
                success: function(data) {
                },
                error: function(data) {
                    alert('no!');
                },
            });
        };
    };

    var show_bulk_export = function() {
        $('#viewer_input').fadeIn();
        $('#viewer_input_heading').html('Export');
        $('#viewer_input_box').focus()
        $.wait(100).then(function() {
            $('#viewer_input_box').val('local');
        });
        $.Images.Viewer.showing_input_box = true;
        $.Images.Viewer.input_box_callback = function() {
            var values = $('#viewer_input_box').val();
            values = values.split();
            values = values.map(function(x) { return x.trim() });
            values = values.filter(function(x) { return x !== "" });
            var export_name = values[0];
            var longest_side = null;
            if (values.length === 2) {
                longest_side = values[1];
            }
            $.ajax({
                url: '/job',
                method: 'POST',
                contentType: "application/json",
                data: JSON.stringify({
                    '*schema': 'Job',
                    method: 'jpeg_export',
                    options: {
                        '*schema': 'JPEGExportOptions',
                        entry_ids: $.map($('.thumb_selected'), function(thumb) { return thumb.getAttribute('data-id'); }),
                        folder: export_name,
                        longest_side: longest_side,
                    },
                }),
                success: function(data) {
                },
                error: function(data) {
                    alert('no!');
                },
            });
        };
    };

    var hide_input_box = function() {
        $('#viewer_input_box').blur();
        $('#viewer_input').fadeOut();
        $.Images.Viewer.showing_input_box = false;
        $.Images.Viewer.input_box_callback = undefined;
    };

    var bind_keys = function() {
        $(document).keydown(function(event) {
            if (event.which === 27) { // escape
                if ($.Images.Viewer.showing_viewer) {
                    if ($.Images.Viewer.showing_metadata) {
                        toggle_metadata();
                        event.preventDefault();
                    } else if ($.Images.Viewer.showing_copies) {
                        toggle_copies();
                        event.preventDefault();
                    } else {
                        hide_viewer();
                        event.preventDefault();
                    }
                } else if ($.Images.Viewer.showing_input_box) {
                    hide_input_box();
                    event.preventDefault();
                } else {
                    back_to_index();
                    event.preventDefault();
                }
            } else if ($('input,textarea').is(':focus')) {
                if (event.which === 13) { // return
                    if ($.Images.Viewer.showing_input_box) {
                        $.Images.Viewer.input_box_callback();
                        hide_input_box();
                        event.preventDefault();
                    }
                }
            } else {
                if (event.which === 37) { // left
                    update_focus({move: -1});
                    event.preventDefault();
                } else if (event.which === 39) { // right
                    update_focus({move: +1});
                    event.preventDefault();
                } else if (event.which === 38) { // up
                    update_focus({move: -10});
                    event.preventDefault();
                } else if (event.which === 40) { // down
                    update_focus({move: +10});
                    event.preventDefault();
                } else if (event.which === 36) { // home
                    update_focus({focus: 0});
                    event.preventDefault();
                } else if (event.which === 35) { // end
                    update_focus({focus: -1});
                    event.preventDefault();
                } else if (event.which === 83) { // space
                    toggle_select();
                    event.preventDefault();
                } else if (!$.Images.Viewer.showing_viewer) {
                    if (event.which === 13) { // return
                        var thumb = $('img.thumb')[$.Images.Viewer.focus];
                        show_viewer({
                            proxy_url: thumb.getAttribute('data-proxy-url'),
                            strip: $.Images.Viewer.fix_strip(thumb.getAttribute('data-strip')),
                        });
                        event.preventDefault();
                    } else if (event.which === 84) { // t
                        show_bulk_add_tags();
                    } else if (event.which === 69) { // e
                        show_bulk_export();
                    } else if (event.which === 65) { // a
                        select_all();
                    } else if (event.which === 78) { // n
                        select_none();
                    }
                } else if ($.Images.Viewer.showing_viewer) {
                    if (event.which === 38) { // up
                        event.preventDefault();
                    } else if (event.which === 40) { // down
                        event.preventDefault();
                    } else if (event.which === 77) { // m
                        toggle_metadata();
                    } else if (event.which === 70) { // f
                        toggle_copies();
                    } else if (event.which === 75) { // k
                        set_state($.Images.Viewer.focus, 'keep');
                    } else if (event.which === 81) { // q
                        set_state($.Images.Viewer.focus, 'keep');
                    } else if (event.which === 88) { // x
                        set_state($.Images.Viewer.focus, 'purge');
                    } else if (event.which === 87) { // w
                        set_state($.Images.Viewer.focus, 'purge');
                    } else if (event.which === 67) { // c
                        toggle_check();
                    } else if (event.which === 84) { // t
                        toggle_metadata(true, 'tags');
                    } else if (event.which === 65) { // a
                        var thumb = $('img.thumb')[$.Images.Viewer.focus];
                        var value = ["brollop_album"];
                        $.ajax({
                            url: '/job',
                            method: 'POST',
                            contentType: "application/json",
                            data: JSON.stringify({
                                '*schema': 'Job',
                                method: 'tag_update',
                                options: {
                                    '*schema': 'TagBulkUpdateOptions',
                                    entry_ids: [thumb.getAttribute('data-id')],
                                    add_tags: value,
                                },
                            }),
                            success: function(data) {
                            },
                            error: function(data) {
                                alert('no!');
                            },
                        });
                    } else if (event.which === 66) { // b
                        var thumb = $('img.thumb')[$.Images.Viewer.focus];
                        var value = ["brollop_album"];
                        $.ajax({
                            url: '/job',
                            method: 'POST',
                            contentType: "application/json",
                            data: JSON.stringify({
                                '*schema': 'Job',
                                method: 'tag_update',
                                options: {
                                    '*schema': 'TagBulkUpdateOptions',
                                    entry_ids: [thumb.getAttribute('data-id')],
                                    remove_tags: value,
                                },
                            }),
                            success: function(data) {
                            },
                            error: function(data) {
                                alert('no!');
                            },
                        });
                    }
                }
            }
        });
    };

    $.Images.Viewer.bind_keys = bind_keys;
    $.Images.Viewer.update_focus = update_focus;
    $.Images.Viewer.show_viewer = show_viewer;
    $.Images.Viewer.create_strip = create_strip;
    $.Images.Viewer.fix_strip = fix_strip;
    $.Images.Viewer.back_to_index = back_to_index;
    $.Images.Viewer.add_thumb = add_thumb;
});
