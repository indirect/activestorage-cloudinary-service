require 'cloudinary'
require 'http'

module ActiveStorage
  class Service::CloudinaryService < Service
    # FIXME: implement setup for private resource type
    # FIXME: allow configuration via cloudinary url
    def initialize(cloud_name:, api_key:, api_secret:, options: {})
      options.merge!(
        cloud_name: cloud_name,
        api_key: api_key,
        api_secret: api_secret
      )
      Cloudinary.config(options)
      # Cloudinary.config_from_url(url)
    end

    def upload(key, io, checksum: nil)
      instrument :upload, key: key, checksum: checksum do
        Cloudinary::Uploader.upload(io, public_id: key)
      end
    end

    # Return the content of the file at the +key+.
    def download(key)
      url = url_for_public_id(key)

      if block_given?
        instrument :streaming_download, key: key do
          body = HTTP.get(url).body
          yield data while (data = body.readpartial)
        end
      else
        instrument :download, key: key do
          HTTP.get(url).to_s
        end
      end
    end

    # Delete the file at the +key+.
    def delete(key)
      instrument :delete, key: key do
        delete_resource_with_public_id(key)
      end
    end

    # Delete files at keys starting with the +prefix+.
    def delete_prefixed(prefix)
      instrument :delete_prefixed, prefix: prefix do
        find_resources_with_public_id_prefix(prefix).each do |resource|
          delete_resource_with_public_id(resource['public_id'])
        end
      end
    end

    # Return +true+ if a file exists at the +key+.
    def exist?(key)
      instrument :exist?, key: key do
        resource_exists_with_public_id?(key)
      end
    end

    # Returns a signed, temporary URL for the file at the +key+. The URL will be valid for the amount
    # of seconds specified in +expires_in+. You must also provide the +disposition+ (+:inline+ or +:attachment+),
    # +filename+, and +content_type+ that you wish the file to be served with on request.
    def url(key, expires_in:, disposition:, filename:, content_type:)
      instrument :url, key: key do
        options = {
          expires_in: expires_in,
          content_type: content_type,
          disposition: disposition,
          filename: filename
        }
        signed_download_url_for_public_id(key, options)
      end
    end

    # Returns a signed, temporary URL that a direct upload file can be POSTed to for the +key+.
    # Cloudinary ignores +expires_in+, +content_type+, +content_length+, and +checksum+.
    # Signed URLs will be valid for exactly one hour.
    def url_for_direct_upload(key, expires_in:, content_type:, content_length:, checksum:)
      instrument :url_for_direct_upload, key: key do
        Cloudinary::Utils.cloudinary_api_url("upload", resource_type: "auto")
      end
    end

    # Returns a Hash of headers for +url_for_direct_upload+ requests.
    def headers_for_direct_upload(key, filename:, content_type:, content_length:, checksum:)
      params = { timestamp: Time.now.to_i, public_id: key }
      api_secret = Cloudinary.config.api_secret
      signature = Cloudinary::Utils.api_sign_request(params, api_secret)

      params.merge(
        api_key: Cloudinary.config.api_key,
        signature: signature,
      )
    end

    private

    def resource_exists_with_public_id?(public_id)
      !find_resource_with_public_id(public_id).empty?
    end

    def find_resource_with_public_id(public_id)
      Cloudinary::Api.resources_by_ids(public_id).fetch('resources')
    end

    def find_resources_with_public_id_prefix(prefix)
      Cloudinary::Api.resources(type: :upload, prefix: prefix).fetch('resources')
    end

    def delete_resource_with_public_id(public_id)
      Cloudinary::Uploader.destroy(public_id)
    end

    def url_for_public_id(public_id)
      Cloudinary::Api.resource(public_id)['secure_url']
    end

    def signed_download_url_for_public_id(public_id, options)
      options[:resource_type] ||= resource_type(options[:content_type])
      Cloudinary::Utils.private_download_url(
        public_id,
        resource_format(options),
        signed_url_options(options)
      )
    end

    def signed_url_options(options)
      {
        resource_type: (options[:resource_type] || 'auto'),
        type: (options[:type] || 'upload'),
        attachment: (options[:attachment] == :attachment),
        expires_at: (Time.now + options[:expires_in])
      }
    end

    def resource_format(_options); end

    def resource_type(content_type)
      content_type.sub(%r{/.*$}, '')
    end
  end
end
