$(function() {
    var load = function(params) {
        params = params || Object();
        var state = params.state || 'final';
        var override = params.override || false;
        $.Images.Viewer.focus = 0;
        if (document.location.hash !== '#' + state) {
            document.location.hash = '#' + state;
        } else if (!override) {
            return;
        }
        $.ajax({
            url: 'entry?state=' + state + '&page_size=500',
            success: function(data) {
                state = data.state;
                $.Images.Viewer.state = state;
                $.Images.Viewer.index_hash = 'dates';
                $('#viewer_feed')
                    .html('');
                $('#state_this')
                    .html("Entries in state " + state)
                    .click(function() { load({'state': state}); });
                $.each(data.entries, $.Images.Viewer.add_thumb);
                $.Images.Viewer.update_focus({focus: 0});
                $.Images.Viewer.total_count = data.count;
            },
        });
    };

    $(document)
        .ready(function() {
            $.Images.Viewer.bind_keys();
            $(window)
                .hashchange(function() {
                    load({
                        state: document.location.hash.substr(1),
                    });
                });
            load({
                state: document.location.hash.substr(1),
                override: true,
            });
        });
});

