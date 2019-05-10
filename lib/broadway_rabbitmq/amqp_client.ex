defmodule BroadwayRabbitMQ.AmqpClient do
  @moduledoc false

  alias AMQP.{
    Connection,
    Channel,
    Basic,
    Queue
  }

  require Logger

  @behaviour BroadwayRabbitMQ.RabbitmqClient

  @default_prefetch_count 50
  @supported_options [
    :queue,
    :exchange,
    :connection,
    :qos,
    :backoff_min,
    :backoff_max,
    :backoff_type,
    :requeue
  ]

  @requeue_options [
    :never,
    :always,
    :once
  ]

  @requeue_default_option :always

  @impl true
  def init(opts) do
    with {:ok, opts} <- validate_supported_opts(opts, "Broadway", @supported_options),
         {:ok, queue} <- validate(opts, :queue),
         {:ok, requeue} <- validate(opts, :requeue, @requeue_default_option),
         {:ok, conn_opts} <- validate_conn_opts(opts[:connection]),
         {:ok, exchange_opts} <-  validate_exchange_opts(opts[:exchange]),
         {:ok, qos_opts} <- validate_qos_opts(opts) do
      {:ok, queue,
       %{
         connection: conn_opts,
         exchange: exchange_opts,
         qos: qos_opts,
         requeue: requeue,
         queue: queue
       }}
    end
  end

  @impl true
  def setup_channel(%{exchange: exchange} = config) when is_nil(exchange) do
    with {:ok, conn} <- Connection.open(config.connection),
         {:ok, channel} <- Channel.open(conn),
         :ok <- Basic.qos(channel, config.qos) do
      {:ok, channel}
    end
  end

  @impl true
  def setup_channel(%{exchange: exchange, queue: queue} = config) do
    with {:ok, conn} <- Connection.open(config.connection),
         {:ok, channel} <- Channel.open(conn),
         :ok <- Queue.bind(channel, queue, exchange[:name], routing_key: "#"),
         :ok <- Basic.qos(channel, config.qos) do
      {:ok, channel}
    end
  end

  @impl true
  def ack(channel, delivery_tag) do
    Basic.ack(channel, delivery_tag)
  end

  @impl true
  def reject(channel, delivery_tag, opts) do
    Basic.reject(channel, delivery_tag, opts)
  end

  @impl true
  def consume(channel, queue) do
    {:ok, consumer_tag} = Basic.consume(channel, queue)
    consumer_tag
  end

  @impl true
  def cancel(channel, consumer_tag) do
    Basic.cancel(channel, consumer_tag)
  end

  @impl true
  def close_connection(conn) do
    if Process.alive?(conn.pid) do
      Connection.close(conn)
    else
      :ok
    end
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:queue, value) when not is_binary(value) or value == "",
    do: validation_error(:queue, "a non empty string", value)

  defp validate_option(:requeue, value) when value not in @requeue_options,
    do: validation_error(:queue, "any of #{inspect(@requeue_options)}", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_conn_opts(conn_opts) when is_nil(conn_opts) do
    {:ok, []}
  end

  defp validate_conn_opts(conn_opts) when is_binary(conn_opts) do
    {:ok, conn_opts}
  end

  defp validate_conn_opts(conn_opts) do
    group = :connection

    supported = [
      :username,
      :password,
      :virtual_host,
      :host,
      :port,
      :channel_max,
      :frame_max,
      :heartbeat,
      :connection_timeout,
      :ssl_options,
      :client_properties,
      :socket_options
    ]

    validate_supported_opts(conn_opts, group, supported)
  end

  defp validate_exchange_opts(nil) do
    {:ok, nil}
  end

  defp validate_exchange_opts(exchange_params) do
    group = :exchange

    exchange_opts = exchange_params[:options] || []

    supported = [
      :passive,
      :durable,
      :auto_delete,
      :internal
    ]

    required = [
      :name,
      :type
    ]

    with {:ok, params} <- validate_required_opts(exchange_params, group, required),
         {:ok, options} <- validate_supported_opts(exchange_opts, group, supported) do
      {:ok, Keyword.put(params, :options, options)}
    end
  end

  defp validate_qos_opts(opts) do
    group = :qos
    qos_opts = opts[group] || []
    supported = [:prefetch_size, :prefetch_count]

    qos_opts
    |> Keyword.put_new(:prefetch_count, @default_prefetch_count)
    |> validate_supported_opts(group, supported)
  end

  defp validate_supported_opts(opts, group_name, supported_opts) do
    opts
    |> Keyword.keys()
    |> Enum.reject(fn k -> k in supported_opts end)
    |> case do
      [] -> {:ok, opts}
      keys -> {:error, "Unsupported options #{inspect(keys)} for #{inspect(group_name)}"}
    end
  end

  defp validate_required_opts(opts, group_name, required_opts) do
    required_opts
    |> Enum.reject(fn k -> k in Keyword.keys(opts) end)
    |> case do
         [] -> {:ok, opts}
         keys -> {:error, "Missing required options #{inspect(keys)} for #{inspect(group_name)}"}
       end
  end
end
