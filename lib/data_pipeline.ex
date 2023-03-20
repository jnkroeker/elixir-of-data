defmodule DataPipeline do
  @moduledoc """
  Documentation for `DataPipeline`.
  """
  use Flow

  @doc """
  The DataPipeline starts with data extraction

  ## Examples

      $ mix compile
      $ iex -S mix

      iex(1)> DataPipeline.get_products()

  """
  def get_products do
    product = extract_product_data('https://dummyjson.com/products')
    |> transform_product_data()
    |> validate_product_data()
    |> load_product_data()
    |> IO.inspect()
  end

  @doc """
  I can fetch data from a database, web service or file system

  One way is to use Flow's from_enumerable/2 function to create a flow from a list or map
  """
  defp extract_product_data(url) do
    extracted_data =
      HTTPoison.get!(url)
      |> Map.get(:body)
      |> Poison.decode!()
      |> Map.get("products")
      |> Flow.from_enumerable()
  end

  @doc """
  Transforming data can involve cleaning, filtering or aggregating

  Flow's map/2 function applies a function to each item in a flow and create a new flow from the results

  Here, use Map to extract description, title and price data into a tuple
  """
  defp transform_product_data(extracted_data) do
    transformed_data = extracted_data
      |> Flow.map(fn product -> {product["description"], product["title"], product["price"]} end)
  end

  @doc """
  Filter out invalid or incomplete data. This stage ensures that the final data is clean and usable.
  """
  defp validate_product_data(transformed_data) do
    validated_data = transformed_data
      |> Flow.filter(fn {description, title, price} -> description != "" and price != "" end)
  end

  @doc """
  The final stage of the pipeline is to load data into a target system; a database, message queue, or writing to a file
  """
  defp load_product_data(validated_data) do
    product_list = validated_data
      |> Flow.map(fn {description, title, price} -> %{title: title, price: price, description: description} end)
      |> Enum.to_list()
  end
end
