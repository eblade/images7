$(function() {
    var load = function(params) {
        params = params || Object();
        var tag = params.tag || 'tag';
        var override = params.override || false;
        $.Images.Viewer.focus = 0;
        if (document.location.hash !== '#' + tag) {
            document.location.hash = '#' + tag;
        } else if (!override) {
            return;
        }
        $.ajax({
            url: 'tag?tag=' + tag + '&page_size=500',
            success: function(data) {
                tag = data.tag;
                $.Images.Viewer.tag = tag;
                $.Images.Viewer.index_hash = 'tags';
                $('#viewer_feed')
                    .html('');
                $('#tag_this')
                    .html(tag)
                    .click(function() { load_tag({'tag': tag}); });
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
                        tag: document.location.hash.substr(1),
                    });
                });
            load({
                tag: document.location.hash.substr(1),
                override: true,
            });
        });
});
