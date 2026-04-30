<script type="text/ecmascript-6">
    import Chart from 'chart.js/auto';

    export default {
        props: ['data'],

        data(){
            return {
                context: null,
                chart:null
            }
        },

        mounted(){
            this.context = this.$refs.canvas.getContext('2d');

            this.chart = new Chart(this.context, {
                type: 'line',
                data: this.data,
                options: {
                    interaction: {
                        intersect: false,
                    },
                    plugins: {
                        legend: {
                            display: false,
                        },
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: (value) => {
                                    return this.data.datasets[0].label === "Seconds"
                                        ? `${value} secs`
                                        : value;
                                },
                            },
                            grid: {
                                display: true,
                            },
                            afterDataLimits(scale) {
                                const max = scale.chart.data.datasets[0].data.reduce(
                                    (currentMax, value) => value > currentMax ? value : currentMax,
                                    0,
                                );

                                scale.max = parseFloat(max) + parseFloat(max * 0.25);
                            },
                        },
                        x: {
                            grid: {
                                display: true,
                            },
                            ticks: {
                                callback(value, index) {
                                    const labels = this.getLabels();

                                    if (index % 6 !== 0 && index + 1 !== labels.length) {
                                        return "";
                                    }

                                    return labels[index];
                                },
                            },
                        },
                    }
                },
            });
        },

        beforeUnmount() {
            this.chart?.destroy();
        },
    }
</script>

<template>
    <div style="position: relative;">
        <canvas ref="canvas" height="120"></canvas>
    </div>
</template>
