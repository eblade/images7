$(function() {
    var imports = function(menu, id) {
        var load = function() {
            $.ajax({
                url: 'importer',
                success: function(data) {
                    $(menu.container_id)
                        .html('<div class="index_sub_menu_strap"></div>');
                    $.each(data.entries, function(index, trigger) {
                        $(menu.container_id)
                            .append('<div id="import_trig_' + trigger.name +
                                    '" class="import_action_button">import from ' + trigger.name +
                                    '</div>');
                        $('#import_trig_' + trigger.name)
                            .click(function() {
                                $.ajax({
                                    url: trigger.trig_url,
                                    method: 'POST',
                                    success: function(data) {
                                        $.monitor(
                                            'importer/status',
                                            'importing',
                                            'scanning',
                                            'importing',
                                            menu.container_id,
                                            menu.close
                                        );
                                    },
                                    error: function(data) {
                                        alert('Error');
                                    },
                                });
                            });
                    });
                },
            });
        };

        menu.register_button('imports', id);
        $(id).click(function(e) {
            menu.toggle('imports', load);
        });
    };

    $.Images = $.Images || {};
    $.Images.imports = imports;
});

